use hashbrown::HashMap;
use slab::Slab;
use std::collections::{BTreeMap, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Market,
    Limit,
    PostOnly,
    IOC,
    FOK,
    MakerOnly,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub id: u64,
    pub user_id: u64,
    pub side: Side,
    pub price: u64,
    pub qty: u64,
    pub remaining: u64,
    pub otype: OrderType,
}

#[derive(Debug, Clone)]
pub struct Fill {
    pub maker_id: u64,
    pub taker_id: u64,
    pub price: u64,
    pub qty: u64,
}

#[derive(Debug, Clone)]
pub enum RiskError {
    PriceOutOfRange,
    PositionLimit,
    RateLimit,
}

pub const PRECISION: u64 = 100_000_000;
pub const MIN_PRICE: u64 = 1 * PRECISION;
pub const MAX_PRICE: u64 = 1_000_000 * PRECISION;

pub struct RiskEngine {
    position_limits: HashMap<u64, u64>,
    rate_limits: HashMap<u64, u64>,
}

impl RiskEngine {
    pub fn new() -> Self {
        RiskEngine {
            position_limits: HashMap::new(),
            rate_limits: HashMap::new(),
        }
    }

    pub fn check_position_limit(&self, user_id: u64, qty: u64) -> bool {
        // 返回 true 表示超限（不能通过）
        self.position_limits
            .get(&user_id)
            .map_or(false, |&limit| qty > limit)
    }

    pub fn check_rate_limit(&mut self, user_id: u64) -> bool {
        //返回 true 表示限频触发（不能通过）
        let remaining = self.rate_limits.entry(user_id).or_insert(1_000_000_000); // 每用户初始 100 次
        if *remaining > 0 {
            *remaining -= 1;
            false // 通过
        } else {
            true // 限频
        }
        // false
    }

    pub fn set_position_limit(&mut self, user_id: u64, max_qty: u64) {
        self.position_limits.insert(user_id, max_qty);
    }
}

pub struct OrderBook {
    bids: BTreeMap<u64, VecDeque<usize>>,
    asks: BTreeMap<u64, VecDeque<usize>>,
    orders: Slab<Order>,
    by_id: HashMap<u64, usize>,
}

impl OrderBook {
    pub fn new() -> Self {
        OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            orders: Slab::new(),
            by_id: HashMap::new(),
        }
    }

    pub fn submit(
        &mut self,
        id: u64,
        user_id: u64,
        otype: OrderType,
        side: Side,
        price: u64,
        qty: u64,
        risk: &mut RiskEngine,
    ) -> Result<Vec<Fill>, RiskError> {
        if price % PRECISION != 0 || qty % PRECISION != 0 || price < MIN_PRICE || price > MAX_PRICE
        {
            return Err(RiskError::PriceOutOfRange);
        }
        if risk.check_position_limit(user_id, qty) {
            return Err(RiskError::PositionLimit);
        }
        if risk.check_rate_limit(user_id) {
            return Err(RiskError::RateLimit);
        }

        let mut fills = Vec::new();
        let mut order = Order {
            id,
            user_id,
            side,
            price,
            qty,
            remaining: qty,
            otype,
        };

        match otype {
            OrderType::Market => {
                self.match_order(&mut order, &mut fills, true);
            }
            OrderType::Limit => {
                self.match_order(&mut order, &mut fills, false);
                if order.remaining > 0 {
                    self.add_to_book(&order);
                }
            }
            OrderType::PostOnly | OrderType::MakerOnly => {
                if self.would_match(&order) {
                    // 不挂单
                } else {
                    self.add_to_book(&order);
                }
            }
            OrderType::IOC => {
                self.match_order(&mut order, &mut fills, false);
            }
            OrderType::FOK => {
                if self.can_full_match(&order) {
                    self.match_order(&mut order, &mut fills, false);
                }
            }
        }

        Ok(fills)
    }

    fn would_match(&self, incoming: &Order) -> bool {
        match incoming.side {
            Side::Bid => self
                .asks
                .keys()
                .next()
                .map_or(false, |&ask| incoming.price >= ask),
            Side::Ask => self
                .bids
                .keys()
                .rev()
                .next()
                .map_or(false, |&bid| incoming.price <= bid),
        }
    }

    fn can_full_match(&self, incoming: &Order) -> bool {
        let mut remaining = incoming.remaining;

        if matches!(incoming.side, Side::Bid) {
            for (&price, queue) in self.asks.iter() {
                if incoming.price < price {
                    break;
                }
                let level_qty: u64 = queue.iter().map(|&idx| self.orders[idx].remaining).sum();
                remaining = remaining.saturating_sub(level_qty);
                if remaining == 0 {
                    return true;
                }
            }
        } else {
            for (&price, queue) in self.bids.iter().rev() {
                if incoming.price > price {
                    break;
                }
                let level_qty: u64 = queue.iter().map(|&idx| self.orders[idx].remaining).sum();
                remaining = remaining.saturating_sub(level_qty);
                if remaining == 0 {
                    return true;
                }
            }
        }
        false
    }

    fn match_order(&mut self, incoming: &mut Order, fills: &mut Vec<Fill>, ignore_price: bool) {
        let mut to_remove_orders = Vec::new();
        let mut prices_to_clean = Vec::new();

        if matches!(incoming.side, Side::Bid) {
            // Bid 吃 Ask (从小到大)
            for (&book_price, queue) in self.asks.iter_mut() {
                if incoming.remaining == 0 {
                    break;
                }
                if !ignore_price && incoming.price < book_price {
                    break;
                }

                let mut indices_to_remove = Vec::new();

                for (pos, &idx) in queue.iter().enumerate() {
                    if incoming.remaining == 0 {
                        break;
                    }

                    let maker = &mut self.orders[idx];
                    if incoming.user_id == maker.user_id {
                        continue;
                    }

                    let fill_qty = incoming.remaining.min(maker.remaining);
                    incoming.remaining -= fill_qty;
                    maker.remaining -= fill_qty;

                    fills.push(Fill {
                        maker_id: maker.id,
                        taker_id: incoming.id,
                        price: book_price,
                        qty: fill_qty,
                    });

                    if maker.remaining == 0 {
                        indices_to_remove.push(pos);
                        to_remove_orders.push(maker.id);
                    }
                }

                for &pos in indices_to_remove.iter().rev() {
                    queue.remove(pos);
                }

                if queue.is_empty() {
                    prices_to_clean.push(book_price);
                }
            }
        } else {
            // Ask 吃 Bid (从大到小)
            for (&book_price, queue) in self.bids.iter_mut().rev() {
                if incoming.remaining == 0 {
                    break;
                }
                if !ignore_price && incoming.price > book_price {
                    break;
                }

                let mut indices_to_remove = Vec::new();

                for (pos, &idx) in queue.iter().enumerate() {
                    if incoming.remaining == 0 {
                        break;
                    }

                    let maker = &mut self.orders[idx];
                    if incoming.user_id == maker.user_id {
                        continue;
                    }

                    let fill_qty = incoming.remaining.min(maker.remaining);
                    incoming.remaining -= fill_qty;
                    maker.remaining -= fill_qty;

                    fills.push(Fill {
                        maker_id: maker.id,
                        taker_id: incoming.id,
                        price: book_price,
                        qty: fill_qty,
                    });

                    if maker.remaining == 0 {
                        indices_to_remove.push(pos);
                        to_remove_orders.push(maker.id);
                    }
                }

                for &pos in indices_to_remove.iter().rev() {
                    queue.remove(pos);
                }

                if queue.is_empty() {
                    prices_to_clean.push(book_price);
                }
            }
        }

        // 统一删除完成的订单
        for id in to_remove_orders {
            self.remove_order(id);
        }

        // 统一清理空的价格层
        for price in prices_to_clean {
            if matches!(incoming.side, Side::Bid) {
                self.asks.remove(&price);
            } else {
                self.bids.remove(&price);
            }
        }
    }

    fn add_to_book(&mut self, order: &Order) {
        let entry = self.orders.vacant_entry();
        let idx = entry.key();
        entry.insert(order.clone());
        self.by_id.insert(order.id, idx);

        let target = match order.side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };

        target
            .entry(order.price)
            .or_insert_with(VecDeque::new)
            .push_back(idx);
    }

    fn remove_order(&mut self, id: u64) {
        if let Some(idx) = self.by_id.remove(&id) {
            self.orders.remove(idx);
        }
    }

    pub fn cancel(&mut self, id: u64) -> Option<Order> {
        if let Some(&idx) = self.by_id.get(&id) {
            let order = self.orders[idx].clone();
            let target = match order.side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };

            if let Some(queue) = target.get_mut(&order.price) {
                if let Some(pos) = queue.iter().position(|&i| i == idx) {
                    queue.remove(pos);
                }
                if queue.is_empty() {
                    target.remove(&order.price);
                }
            }

            self.remove_order(id);
            Some(order)
        } else {
            None
        }
    }

    pub fn modify(&mut self, id: u64, new_price: Option<u64>, new_qty: Option<u64>) {
        if let Some(mut order) = self.cancel(id) {
            if let Some(p) = new_price {
                order.price = p;
            }
            if let Some(q) = new_qty {
                order.qty = q;
                order.remaining = q;
            }
            self.add_to_book(&order);
        }
    }

    pub fn batch_submit(
        &mut self,
        orders: Vec<(u64, u64, OrderType, Side, u64, u64)>,
        risk: &mut RiskEngine,
    ) -> Vec<Result<Vec<Fill>, RiskError>> {
        orders
            .into_iter()
            .map(|(id, uid, ot, side, p, q)| self.submit(id, uid, ot, side, p, q, risk))
            .collect()
    }

    pub fn get_l2_snapshot(&self, depth: usize) -> (Vec<(u64, u64)>, Vec<(u64, u64)>) {
        let mut bids = Vec::with_capacity(depth.min(self.bids.len()));
        let mut bid_iter = self.bids.iter().rev();
        for _ in 0..depth {
            if let Some((&price, queue)) = bid_iter.next() {
                let total: u64 = queue.iter().map(|&idx| self.orders[idx].remaining).sum();
                bids.push((price, total));
            } else {
                break;
            }
        }

        let mut asks = Vec::with_capacity(depth.min(self.asks.len()));
        let mut ask_iter = self.asks.iter();
        for _ in 0..depth {
            if let Some((&price, queue)) = ask_iter.next() {
                let total: u64 = queue.iter().map(|&idx| self.orders[idx].remaining).sum();
                asks.push((price, total));
            } else {
                break;
            }
        }

        (bids, asks)
    }
}

pub struct MatchingEngine {
    books: HashMap<String, OrderBook>,
    risk: RiskEngine,
}

impl MatchingEngine {
    pub fn new() -> Self {
        MatchingEngine {
            books: HashMap::new(),
            risk: RiskEngine::new(),
        }
    }

    pub fn set_rate_limit(&mut self, user_id: u64, limit: u64) {
        self.risk.rate_limits.insert(user_id, limit);
    }

    pub fn submit(
        &mut self,
        symbol: &str,
        id: u64,
        user_id: u64,
        otype: OrderType,
        side: Side,
        price: u64,
        qty: u64,
    ) -> Result<Vec<Fill>, RiskError> {
        let book = self
            .books
            .entry(symbol.to_string())
            .or_insert_with(OrderBook::new);
        book.submit(id, user_id, otype, side, price, qty, &mut self.risk)
    }

    pub fn cancel(&mut self, symbol: &str, id: u64) -> Option<Order> {
        self.books.get_mut(symbol).and_then(|book| book.cancel(id))
    }

    pub fn modify(&mut self, symbol: &str, id: u64, new_price: Option<u64>, new_qty: Option<u64>) {
        if let Some(book) = self.books.get_mut(symbol) {
            book.modify(id, new_price, new_qty);
        }
    }

    pub fn batch_submit(
        &mut self,
        symbol: &str,
        orders: Vec<(u64, u64, OrderType, Side, u64, u64)>,
    ) -> Vec<Result<Vec<Fill>, RiskError>> {
        self.books
            .get_mut(symbol)
            .map_or(vec![], |book| book.batch_submit(orders, &mut self.risk))
    }

    pub fn get_l2_snapshot(
        &self,
        symbol: &str,
        depth: usize,
    ) -> Option<(Vec<(u64, u64)>, Vec<(u64, u64)>)> {
        self.books
            .get(symbol)
            .map(|book| book.get_l2_snapshot(depth))
    }

    pub fn set_position_limit(&mut self, user_id: u64, max_qty: u64) {
        self.risk.set_position_limit(user_id, max_qty);
    }
}
