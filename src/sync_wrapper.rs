/// Inspired by https://internals.rust-lang.org/t/what-shall-sync-mean-across-an-await/12020/2.
#[repr(transparent)]
#[derive(Debug)]
pub struct SyncWrapper<T>(T);

impl<T> SyncWrapper<T> {
    /// Create a new [`SyncWrapper`] over an item.
    pub(crate) fn new(item: T) -> Self {
        Self(item)
    }

    /// Consume this object to access the inner item.
    pub fn into_inner(self) -> T {
        self.0
    }
}

// Safety:
// * This is safe as this type is useless without an immutable reference.
unsafe impl<T> Sync for SyncWrapper<T> where T: Send {}
