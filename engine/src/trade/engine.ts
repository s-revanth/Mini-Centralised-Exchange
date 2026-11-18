import dotenv from "dotenv";
import { Order, OrderBook } from "./orderBooks";
import { RedisManager } from "../redis/redis";
import { randomUUID } from "crypto";
import { EngineEvent, EngineStore } from "../persistence/store";
dotenv.config();
interface UserBalance {
  [key: string]: {
    available: number,
    locked: number
  }
}
const BASE_CURRENCY = "USDC"
export class Engine {
  private orderBooks: any = [];
  private balances: Map<string, UserBalance> = new Map();
  private store: EngineStore;
  private seq = 0; // seq of the last event applied to in-memory state
  private snapshotSeq = 0; // seq covered by the last snapshot on disk

  constructor() {
    this.store = new EngineStore(process.env.ENGINE_DATA_DIR || "./data");
    this.recover();
    const interval = Number(process.env.SNAPSHOT_INTERVAL_MS) || 1000 * 3;
    setInterval(() => {
      this.saveSnapshot();
    }, interval);
  }

  // latest snapshot (or genesis balances) + replay of every logged event after it
  recover() {
    const started = Date.now();
    const snapShot = this.store.loadSnapshot();
    if (snapShot) {
      this.orderBooks = snapShot.orderbooks.map((o: any) => new OrderBook(o.baseAsset, o.bids, o.asks, o.lastTradeId, o.currentPrice));
      this.balances = new Map(snapShot.balances);
      this.seq = this.snapshotSeq = snapShot.lastSeq;
    }
    else {
      this.orderBooks = [new OrderBook(`SOL`, [], [], 0, 0)];
      this.setBaseBalances();
    }
    const events = this.store.readEvents();
    let replayed = 0;
    let mismatches = 0;
    for (const event of events) {
      if (event.seq <= this.seq) {
        continue;
      }
      if (event.seq !== this.seq + 1) {
        throw new Error(`event log gap: expected seq ${this.seq + 1}, found ${event.seq}`);
      }
      if (!this.replay(event)) {
        mismatches++;
      }
      this.seq = event.seq;
      replayed++;
    }
    console.log(`[recovery] done: snapshot ${snapShot ? "seq=" + snapShot.lastSeq : "none"}, ` +
      `log has ${events.length} events, replayed ${replayed} (mismatches=${mismatches}) in ${Date.now() - started} ms, now at seq=${this.seq}`);
  }

  // re-applies a logged event; returns false if the outcome differs from the log
  replay(event: EngineEvent): boolean {
    const data = event.data;
    let ok = true;
    let fills: any[] = [];
    try {
      switch (event.type) {
        case "CREATE_ORDER":
          fills = this.createOrder(data.userId, data.price, data.quantity, data.market, data.side, data.orderId).fills;
          break;
        case "CANCEL_ORDER":
          this.cancelOrder(data.orderId, data.market, data.userId);
          break;
        case "ADD_BALANCE":
          this.addBalance(data.userId, data.amount);
          break;
      }
    } catch (e) {
      ok = false;
    }
    const same = ok === event.ok && JSON.stringify(fills) === JSON.stringify(event.fills || []);
    if (!same) {
      console.log(`[recovery] replay of seq=${event.seq} did not reproduce the logged outcome`);
    }
    return same;
  }

  // durably record a state change before any of its effects leave the engine
  persist(type: EngineEvent["type"], data: any, ok: boolean, fills?: any[]) {
    const event: EngineEvent = { seq: this.seq + 1, ts: Date.now(), type, data, ok };
    if (fills) {
      event.fills = fills;
    }
    this.store.append(event);
    this.seq = event.seq;
  }

  saveSnapshot() {
    if (this.seq === this.snapshotSeq) {
      return; // nothing changed since the last snapshot
    }
    const snapshotSnapshot = {
      lastSeq: this.seq,
      takenAt: Date.now(),
      orderbooks: this.orderBooks.map((o: any) => o.getSnap()),
      balances: Array.from(this.balances.entries())
    }
    //should save to s3
    this.store.writeSnapshot(snapshotSnapshot);
    this.snapshotSeq = snapshotSnapshot.lastSeq;
    console.log(`[snapshot] wrote seq=${snapshotSnapshot.lastSeq}`);
  }
  setBaseBalances() {
    this.balances.set("1", {
      [BASE_CURRENCY]: {
        available: 50000,
        locked: 0
      },
      "SOL": {
        available: 50000,
        locked: 0
      }
    });

    this.balances.set("2", {
      [BASE_CURRENCY]: {
        available: 50000,
        locked: 0
      },
      "SOL": {
        available: 50000,
        locked: 0
      }
    });
  }

  process({ message, clientId }: any) {
    switch (message.type) {
      case 'CREATE_ORDER': {
        // the id is part of the logged event so replay recreates the same order
        const data = { ...message.data, orderId: randomUUID() };
        let result;
        try {
          result = this.createOrder(data.userId, data.price, data.quantity, data.market, data.side, data.orderId);
        } catch (error) {
          console.log("Error in creating order in the engine", error);
        }
        this.persist("CREATE_ORDER", data, !!result, result ? result.fills : []);
        try {
          if (!result) {
            throw new Error("order rejected");
          }
          const { executed, fills, orderId } = result;
          this.publisWsDepthUpdates(fills, data.price, data.side, data.market);
          this.publishWsTrades(fills, data.userId, data.market);
          this.createDbTrades(fills, data.market, data.userId);
          RedisManager.getInstance().sendToApi(clientId, {
            type: "ORDER_PLACED",
            payload: {
              orderId,
              executed,
              fills
            }
          });
        } catch (error) {
          RedisManager.getInstance().sendToApi(clientId, {
            type: "ORDER_CANCELLED",
            payload: {
              orderId: "",
              executed: 0,
              remaining: 0
            }
          })
        }
        break;
      }
      case 'CANCEL_ORDER': {
        const orderId = message.data.orderId;
        const cancelMarket = message.data.market;
        let price;
        let ok = true;
        try {
          price = this.cancelOrder(orderId, cancelMarket, message.data.userId);
        } catch (error) {
          ok = false;
          console.log("Error in cancelling order", error);
        }
        this.persist("CANCEL_ORDER", message.data, ok);
        if (!ok) {
          break;
        }
        try {
          if (price) {
            this.sendUpdatedDepthAt(price.toString(), cancelMarket);
          }
          RedisManager.getInstance().sendToApi(clientId, {
            type: "ORDER_CANCELLED",
            payload: {
              orderId,
              executedQty: 0,
              remainingQty: 0
            }
          });


        } catch (error) {
          console.log("Error in cancelling order", error);
        }
        break;
      }
      case 'GET_OPEN_ORDERS':
        try {
          const openOrderbook = this.orderBooks.find((o: any) => o.ticker() === message.data.market);
          if (!openOrderbook) {
            throw new Error("No orderbook found");
          }
          const openOrders = openOrderbook.getOpenOrders(message.data.userId);

          RedisManager.getInstance().sendToApi(clientId, {
            type: "OPEN_ORDERS",
            payload: openOrders
          });
        } catch (error) {
          console.log("ERROR IN GETTING OPEN ORDERS", error);
        }
        break;
      case 'GET_DEPTH':
        try {
          const market = message.data.market;
          const orderbook = this.orderBooks.find((o: any) => o.ticker() === market);
          if (!orderbook) {
            throw new Error("No orderbook found");
          }
          RedisManager.getInstance().sendToApi(clientId, {
            type: "DEPTH",
            payload: orderbook.getDepth()
          });
        } catch (error) {
          console.log("Error in getting market depth", error);
        }
        break;
      case 'GET_BALANCE':
        RedisManager.getInstance().sendToApi(clientId, {
          type: "BALANCE",
          payload: this.balances.get(message.data.userId) || {}
        });
        break;
      case 'ADD_BALANCE': {
        let ok = true;
        try {
          this.addBalance(message.data.userId, message.data.amount);
        } catch (error) {
          ok = false;
          console.log("Error in adding funds", error);
        }
        this.persist("ADD_BALANCE", message.data, ok);
      }
    }
  }

  addBalance(userId: string, rawAmount: string) {
    const amount = Number(rawAmount);
    const userBalance = this.balances.get(userId);
    if (!userBalance) {
      this.balances.set(userId, {
        [BASE_CURRENCY]: {
          available: amount,
          locked: 0
        }
      });
    } else {
      userBalance[BASE_CURRENCY]!.available += amount;
    }
  }

  // mutates the book and balances only; the caller publishes depth updates
  cancelOrder(orderId: string, cancelMarket: string, userId: string) {
    const cancelOrderbook = this.orderBooks.find((o: any) => o.ticker() === cancelMarket);
    if (!cancelOrderbook) {
      throw new Error("No orderbook found");
    }
    const quoteAsset = cancelMarket.split("_")[1];
    const order = cancelOrderbook.asks.find((o: Order) => o.orderId === orderId) || cancelOrderbook.bids.find((o: Order) => o.orderId === orderId);
    if (order.userId != userId) {
      throw new Error("Unauthorized: cannot cancel others' orders");
    }
    if (!order) {
      console.log("No order found");
      throw new Error("No order found");
    }
    if (order.side === "buy") {
      const price = cancelOrderbook.cancelBuys(order)
      const leftQuantityPrice = (order.quantity - order.filled) * order.price;
      //@ts-ignore
      this.balances.get(order.userId)[BASE_CURRENCY].available += leftQuantityPrice;
      //@ts-ignore
      this.balances.get(order.userId)[BASE_CURRENCY].locked -= leftQuantityPrice;
      return price;
    } else {
      const price = cancelOrderbook.cancelSells(order)
      const leftQuantity = order.quantity - order.filled;
      //@ts-ignore
      this.balances.get(order.userId)[quoteAsset].available += leftQuantity;
      //@ts-ignore
      this.balances.get(order.userId)[quoteAsset].locked -= leftQuantity;
      return price;
    }
  }


  // mutates the book and balances only; the caller publishes trades/depth after logging
  createOrder(userId: string, price: string, quantity: string, market: string, side: "BUY" | "SELL", orderId: string) {
    const orderBook = this.orderBooks.find((o: any) => { return o.ticker() == market });
    const baseAsset = market.split("_")[0]!;
    const quoteAsset = market.split("_")[1]!;
    if (!orderBook) {
      throw new Error("No orderbook found");
    }
    this.checkAndUpdateFunds(baseAsset, quoteAsset, side, userId, quoteAsset, price, quantity);
    const order = {
      price: Number(price),
      quantity: Number(quantity),
      orderId: orderId,
      filled: 0,
      side: side,
      userId: userId
    }
    const { fills, executed } = orderBook.createOrder(order);
    this.updateFunds(userId, baseAsset, quoteAsset, side, fills, executed);
    return { executed, fills, orderId: order.orderId };
  }

  checkAndUpdateFunds(baseAsset: string, quoteAsset: string, side: "BUY" | "SELL", userId: string, asset: string, price: string, quantity: string) {
    const userBalance = this.balances.get(userId);
    if (!userBalance) {
      throw new Error("User balance not found");
    }
    if (side == "BUY") {

      if ((userBalance[quoteAsset]?.available || 0) < Number(quantity) * Number(price)) {
        throw new Error("Insufficient balance");
      }
      userBalance[quoteAsset]!.available -= Number(quantity) * Number(price);
      userBalance[quoteAsset]!.locked += Number(quantity) * Number(price);

      //@ts-ignore
      //this.balances.get(userId)[quoteAsset]?.available = this.balances.get(userId)?.[quoteAsset].available - (Number(quantity) * Number(price));
      //@ts-ignore
      //this.balances.get(userId)[quoteAsset].locked = this.balances.get(userId)?.[quoteAsset].locked + (Number(quantity) * Number(price));
    }
    else {

      if ((userBalance[baseAsset]?.available || 0) < Number(quantity)) {
        throw new Error("Insufficient funds");
      }
      userBalance[baseAsset]!.available -= Number(quantity);
      userBalance[baseAsset]!.locked += Number(quantity);

      //@ts-ignore
      //this.balances.get(userId)[baseAsset].available = this.balances.get(userId)?.[baseAsset].available - (Number(quantity));
      //@ts-ignore
      //this.balances.get(userId)[baseAsset].locked = this.balances.get(userId)?.[baseAsset].locked + Number(quantity);
    }
  }
  updateFunds(userId: string, baseAsset: string, quoteAsset: string, side: "BUY" | "SELL", fills: any, executed: number) {
    try {
      if (side == "BUY") {
        fills.forEach((fill: any) => {
          //@ts-ignore
          // credit seller INR;
          this.balances.get(fill.otherUserId)[quoteAsset].available = this.balances.get(fill.otherUserId)?.[quoteAsset].available + (fill.qty * fill.price);
          //@ts-ignore
          //Debit buyer locked INR
          this.balances.get(userId)[quoteAsset].locked = this.balances.get(userId)?.[quoteAsset].locked - (fill.qty * fill.price);
          //@ts-ignore
          //debit seller locked stocks
          this.balances.get(fill.otherUserId)[baseAsset].locked = this.balances.get(fill.otherUserId)?.[baseAsset].locked - fill.qty;
          //@ts-ignore
          // credit buyer stocks
          this.balances.get(userId)[baseAsset].available = this.balances.get(userId)?.[baseAsset].available + fill.qty;
        });
      }
      else {
        fills.forEach((fill: any) => {
          //@ts-ignore
          // buyers money(other user) got subtracted since he/she has to buy the stock
          this.balances.get(fill.otherUserId)[quoteAsset].locked = this.balances.get(fill.otherUserId)?.[quoteAsset].locked - (fill.qty * fill.price);
          //@ts-ignore
          //user who was selling got his money  
          this.balances.get(userId)[quoteAsset].available = this.balances.get(userId)?.[quoteAsset].available + (fill.qty * fill.price);
          //@ts-ignore
          //buyer's stock balance is incrased
          this.balances.get(fill.otherUserId)[baseAsset].available = this.balances.get(fill.otherUserId)?.[baseAsset].available + fill.qty;
          //@ts-ignore
          //seller stock balance decreased
          this.balances.get(userId)[baseAsset].locked = this.balances.get(userId)?.[baseAsset].locked - (fill.qty);
        });
      }
    } catch (error) {
      console.log("ERROR IN UPDATING FUNDS");
    }
  }
  sendUpdatedDepthAt(price: string, market: string) {
    const orderbook = this.orderBooks.find((o: any) => o.ticker() === market);
    if (!orderbook) {
      return;
    }
    const depth = orderbook.getDepth();
    const updatedBids = depth?.bids.filter((x: any) => x[0] === price);
    const updatedAsks = depth?.asks.filter((x: any) => x[0] === price);

    RedisManager.getInstance().publishMessage(`depth@${market}`, {
      stream: `depth@${market}`,
      data: {
        a: updatedAsks.length ? updatedAsks : [[price, "0"]],
        b: updatedBids.length ? updatedBids : [[price, "0"]],
        e: "depth"
      }
    });
  }
  publishWsTrades(fills: any, userId: string, market: string) {
    console.log("publishing to user via trades handler")
    fills.forEach((fill: any) => {
      RedisManager.getInstance().publishMessage(`trade@${market}`, {
        stream: `trade@${market}`,
        data: {
          e: "trade",
          t: fill.tradeId,
          m: fill.otherUserId === userId,
          p: fill.price,
          q: fill.qty.toString(),
          s: market,
        }
      });
    });
    console.log("published ws trades");
  }
  publisWsDepthUpdates(fills: any, price: string, side: "BUY" | "SELL", market: string) {
    console.log(market);
    const orderbook = this.orderBooks.find((o: any) => o.ticker() === market);
    if (!orderbook) {
      return;
    }
    console.log("publishing to user in ws depth");
    const depth = orderbook.getDepth();
    if (side === "BUY") {
      console.log("insdie ws depth buy")
      const updatedAsks = depth?.asks.filter((x: any) => fills.map((f: any) => f.price).includes(x[0].toString()));
      const updatedBid = depth?.bids.find((x: any) => x[0] === price);
      RedisManager.getInstance().publishMessage(`depth@${market}`, {
        stream: `depth@${market}`,
        data: {
          a: updatedAsks,
          b: updatedBid ? [updatedBid] : [],
          e: "depth"
        }
      });
    }
    if (side === "SELL") {
      const updatedBids = depth?.bids.filter((x: any) => fills.map((f: any) => f.price).includes(x[0].toString()));
      const updatedAsk = depth?.asks.find((x: any) => x[0] === price);
      RedisManager.getInstance().publishMessage(`depth@${market}`, {
        stream: `depth@${market}`,
        data: {
          a: updatedAsk ? [updatedAsk] : [],
          b: updatedBids,
          e: "depth"
        }
      });
    }
  }
  createDbTrades(fills: any, market: string, userId: string) {
    console.log("db trade createdd");
    fills.forEach((fill: any) => {
      RedisManager.getInstance().pushMessage({
        type: "TRADE_ADDED",
        data: {
          market: market,
          id: fill.tradeId.toString(),
          isBuyerMaker: fill.otherUserId === userId,
          price: fill.price,
          quantity: fill.qty.toString(),
          quoteQuantity: (fill.qty * Number(fill.price)).toString(),
          timestamp: Date.now()
        }
      });
    });
  }

}
