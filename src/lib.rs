//! A concurrent queue that uses a ring buffer to store its values.

#![feature(
    box_syntax,
    maybe_uninit_ref,
    maybe_uninit_uninit_array,
    min_const_generics,
    thread_spawn_unchecked
)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

use std::{fmt::Debug, mem::{self, MaybeUninit}, sync::{Condvar, Mutex}};

/// A concurrent fixed-size queue.
#[derive(Debug)]
pub struct RingQueue<T, const LEN: usize> {
    // All the stuff that needs to be synchronized.
    inner: Mutex<Inner<T, LEN>>,
    // The condition to wait on in the `pop` function.
    pop_cond: Condvar,
    // The condition to wait on in the `push` function.
    push_cond: Condvar,
}

impl<T, const LEN: usize> RingQueue<T, LEN>
where
    T: Debug,
{
    /// Create a new `RingQueue`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Gets the first value out of the queue. Blocks while the queue is empty.
    pub fn pop(&self) -> T {
        let mut inner = self.pop_cond
            .wait_while(self.inner.lock().unwrap(), |inner| inner.size == 0)
            .unwrap();
        let ret = inner.pop();
        self.push_cond.notify_one();
        println!("Popping {:?} from queue", ret);
        ret
    }

    /// Adds a new value to the end of the queue. Blocks while the queue is full.
    pub fn push(&self, value: T) {
        println!("Pushing {:?} into queue", value);
        let mut inner = self.push_cond
            .wait_while(self.inner.lock().unwrap(), |inner| inner.size == LEN)
            .unwrap();
        inner.push(value);
        self.pop_cond.notify_one();
    }
}

// SAFETY: Calling this function is only safe when `value` is initialized.
unsafe fn clone_initialized_uninit<T>(value: &MaybeUninit<T>) -> MaybeUninit<T>
where
    T: Clone,
{
    MaybeUninit::new(value.assume_init_ref().clone())
}

impl<T, const LEN: usize> Clone for RingQueue<T, LEN>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        let inner = self.inner.lock().unwrap().clone();
        Self {
            inner: Mutex::new(inner),
            pop_cond: Condvar::new(),
            push_cond: Condvar::new(),
        }
    }
}

impl<T, const LEN: usize> Default for RingQueue<T, LEN> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            pop_cond: Default::default(),
            push_cond: Default::default(),
        }
    }
}

// SAFETY: This impl is safe because all accesses to `inner` -- which is the only `!Sync` field in
//         `RingQueue` -- are done either while holding `lock` or before any reference to `self`
//         can be available to other threads.
unsafe impl<T, const LEN: usize> Sync for RingQueue<T, LEN> {}

#[derive(Debug)]
struct Inner<T, const LEN: usize> {
    // INVARIANT: Starting at index `self.start` and wrapping around from the end of the queue to
    //            the beginning such that `LEN` is identified with 0, the first `self.size`
    //            elements of `self.values` are always initialized. No guarantee is made about
    //            whether any other elements are initialized.
    values: [MaybeUninit<T>; LEN],
    // The index of the next value to return.
    start: usize,
    // The index of the next value to overwrite.
    size: usize,
}

impl<T, const LEN: usize> Inner<T, LEN> {
    pub fn pop(&mut self) -> T {
        // This method upholds the invariant on `self.values` because it reduces the size of the
        // window covered by the invariant then shifts it so that it includes all and only those
        // elements which were previously included and will not be returned.
        self.size -= 1;
        let old_start = self.start;
        self.start = (self.start + 1) % LEN;
        let ret = mem::replace(&mut self.values[old_start], MaybeUninit::uninit());
        // SAFETY: This use of `clone_initialized_uninit` is safe because it is an invariant that
        //         the first `self.size` values logically after `self.start` are initialized.
        unsafe {
            ret.assume_init()
        }
    }

    pub fn push(&mut self, value: T) {
        // This method upholds the invariant on `self.values` because it inserts the new value as
        // the first element after the end of the window covered by the invariant before increasing
        // the size of that window.
        let end = if self.size >= LEN - self.start {
            self.size - (LEN - self.start)
        } else {
            self.start + self.size
        };
        self.values[end] = MaybeUninit::new(value);
        self.size += 1;
    }
}

impl<T, const LEN: usize> Clone for Inner<T, LEN>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        let mut values = MaybeUninit::uninit_array();
        if self.size > LEN - self.start {
            for i in (self.start..LEN).chain(0..(self.size - (LEN - self.start))) {
                // SAFETY: This use of `clone_initialized_uninit` is safe because it is an invariant
                //         that the first `self.size` values after `self.start` are initialized.
                values[i] = unsafe { clone_initialized_uninit(&self.values[i]) };
            }
        } else {
            for i in self.start..(self.start + self.size) {
                // SAFETY: This use of `clone_initialized_uninit` is safe because it is an invariant
                //         that the first `self.size` values after `self.start` are initialized.
                values[i] = unsafe { clone_initialized_uninit(&self.values[i]) };
            }
        }
        Self {
            values,
            start: self.start,
            size: self.size,
        }
    }
}

impl<T, const LEN: usize> Default for Inner<T, LEN> {
    fn default() -> Self {
        Self {
            values: MaybeUninit::uninit_array(),
            start: 0,
            size: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::thread::Builder as ThreadBuilder;

    #[test]
    fn it_works() {
        println!("Creating queue");
        let queue = RingQueue::<u32, 1>::new();
        println!("Pushing to queue");
        queue.push(3);
        println!("Popping from queue");
        assert_eq!(queue.pop(), 3);
    }

    #[test]
    fn it_works_across_threads() {
        let queue = RingQueue::<u32, 10>::new();
        // SAFETY: This call to `spawn_unchecked` is safe because its only reference to this thread
        //         is `queue`, which is dropped after `sender` is `join`ed.
        let sender = unsafe {
            ThreadBuilder::new().name("Sender".into()).spawn_unchecked(|| {
                for i in 0..10 {
                    queue.push(i);
                }
            }).unwrap()
        };
        // SAFETY: This call to `spawn_unchecked` is safe because its only reference to this thread
        //         is `queue`, which is dropped after `receiver` is `join`ed.
        let receiver = unsafe {
            ThreadBuilder::new().name("Receiver".into()).spawn_unchecked(|| {
                let mut ret = vec![];
                for _ in 0..10 {
                    ret.push(queue.pop());
                }
                return ret;
            }).unwrap()
        };
        sender.join().unwrap();
        let received = receiver.join().unwrap();
        assert_eq!(received, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn it_works_with_multiple_senders() {
        let queue = RingQueue::<u32, 10>::new();
        // SAFETY: This call to `spawn_unchecked` is safe because its only reference to this thread
        //         is `queue`, which is dropped after `sender` is `join`ed.
        let sender1 = unsafe {
            ThreadBuilder::new().name("Sender1".into()).spawn_unchecked(|| {
                for i in 0..10 {
                    queue.push(i);
                }
            }).unwrap()
        };
        // SAFETY: This call to `spawn_unchecked` is safe because its only reference to this thread
        //         is `queue`, which is dropped after `sender` is `join`ed.
        let sender2 = unsafe {
            ThreadBuilder::new().name("Sender2".into()).spawn_unchecked(|| {
                for i in 10..20 {
                    queue.push(i);
                }
            }).unwrap()
        };
        // SAFETY: This call to `spawn_unchecked` is safe because its only reference to this thread
        //         is `queue`, which is dropped after `receiver` is `join`ed.
        let receiver = unsafe {
            ThreadBuilder::new().name("Receiver".into()).spawn_unchecked(|| {
                let mut ret = vec![];
                for _ in 0..20 {
                    ret.push(queue.pop());
                }
                return ret;
            }).unwrap()
        };
        sender1.join().unwrap();
        sender2.join().unwrap();
        let received = receiver.join().unwrap();
        assert_eq!(
            received.iter().copied().filter(|&x| x < 10).collect::<Vec<_>>(),
            (0..10).collect::<Vec<_>>(),
        );
        assert_eq!(
            received.into_iter().filter(|&x| x >= 10).collect::<Vec<_>>(),
            (10..20).collect::<Vec<_>>(),
        );
    }
}
