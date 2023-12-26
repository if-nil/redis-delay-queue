use uuid::Uuid;
#[derive(Eq)]
pub struct Item {
    id: String,
    delay: u64,
    data: String,
}

impl Item {
    fn new(delay: u64, data: String) -> Self {
        let id = Uuid::new_v4().to_string();
        Self { id, delay, data }
    }
}

impl PartialEq for Item {
    fn eq(&self, other: &Item) -> bool {
        self.delay == other.delay
    }
}

impl Ord for Item {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.delay.cmp(&other.delay)
    }
}

impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
