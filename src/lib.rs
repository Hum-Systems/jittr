use futures::sink::Sink;
use futures::{FutureExt, Stream};
use futures_timer::Delay;
use std::collections::BinaryHeap;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, SystemTime};

fn interpolation<P: Packet>(left: &P, _: &P) -> Option<P> {
    Some(left.clone())
}

pub struct JitterBuffer<P, const S: usize>
where
    P: Packet,
{
    last: Option<JitterPacket<P>>,
    delay: Option<Delay>,

    queued: Option<P>,
    heap: BinaryHeap<JitterPacket<P>>,

    // settings
    interpolation: fn(&P, &P) -> Option<P>,
    sample_rate: usize,
    channels: usize,

    producer: Option<Waker>,
    consumer: Option<Waker>,
}

impl<P, const S: usize> JitterBuffer<P, S>
where
    P: Packet,
{
    pub fn new(sample_rate: usize, channels: usize) -> Self {
        Self {
            last: None,
            delay: None,

            queued: None,
            heap: BinaryHeap::with_capacity(S),

            interpolation,
            sample_rate,
            channels,

            producer: None,
            consumer: None,
        }
    }

    pub fn with_interpolation(mut self, interpolation: fn(&P, &P) -> Option<P>) -> Self {
        self.interpolation = interpolation;
        self
    }

    /// Returns the calcualted packet loss ratio in this moment
    pub fn plr(&self) -> f32 {
        let buffered = self.heap.len();
        let packets_lost = self
            .heap
            .iter()
            .fold((0, 0), |(lost, last_seq), packet| {
                let current = packet.raw.sequence_number();

                if last_seq == 0 {
                    return (lost, current);
                }

                if last_seq + 1 != current {
                    return (lost + 1, current);
                }

                (lost, current)
            })
            .0;

        //println!("plr {}", packets_lost as f32 / buffered as f32);

        packets_lost as f32 / buffered as f32
    }
}

impl<P, const S: usize> Sink<P> for JitterBuffer<P, S>
where
    P: Packet,
{
    type Error = ();

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.queued.is_some() {
            return Poll::Pending;
        }

        Poll::Ready(Ok(()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if let Some(packet) = self.queued.take() {
            if self.heap.len() >= S {
                self.queued = Some(packet);
                self.producer = Some(cx.waker().clone());
                return Poll::Pending;
            }

            if let Some(ref last) = self.last {
                if last.raw.sequence_number() >= packet.sequence_number() {
                    // discarded packet since we played back a later one already
                    return Poll::Ready(Ok(()));
                }
            }

            if self
                .heap
                .iter()
                .any(|p| p.raw.sequence_number() == packet.sequence_number())
            {
                // discarded packet since we already have it in the heap
                return Poll::Ready(Ok(()));
            }

            self.heap.push(packet.into());

            if let Some(ref consumer) = self.consumer {
                consumer.wake_by_ref();
            }
        }

        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: P) -> Result<(), Self::Error> {
        if self.queued.is_some() {
            return Err(());
        }

        self.queued = Some(item);

        Ok(())
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}

impl<P, const S: usize> Stream for JitterBuffer<P, S>
where
    P: Packet,
{
    type Item = P;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.consumer.replace(cx.waker().clone());

        if self.heap.is_empty() {
            if let Some(ref producer) = self.producer {
                producer.wake_by_ref();
            }

            return Poll::Pending;
        }

        // check if we have enough packets in the jitter to fight network jitter
        // this amount should be calcualted based on network latency! find an algorithm for
        // delaying playback!

        if self.heap.len() < (S as f32 * self.plr()) as usize {
            if let Some(ref producer) = self.producer {
                producer.wake_by_ref();
            }

            return Poll::Pending;
        }

        let last = match self.last {
            Some(ref last) => last.to_owned(),
            // no need to delay until the last packet is played back since
            // we are yielding the first packet right now
            None => {
                // SAFETY:
                // we checked that the heap is not empty so at least one
                // element must be present or the std implementation is flawed.
                let mut packet = self.heap.pop().unwrap();
                packet.yieleded_at = Some(SystemTime::now());
                self.last = Some(packet.clone());

                //println!("yielding first packet: sn {}", packet.raw.sequence_number());

                return Poll::Ready(Some(packet.into()));
            }
        };

        //println!(
        //"we have last: sn {} with offset {} and samples {} yielded at {:?}",
        //last.raw.sequence_number(),
        //last.raw.offset(),
        //last.raw.samples(),
        //last.yieleded_at.unwrap().elapsed().unwrap()
        //);

        // we handed a packet before, lets sleep if it is played back completly
        match self.delay.as_mut() {
            Some(ref mut delay) => match delay.poll_unpin(cx) {
                Poll::Ready(_) => {
                    self.delay = None;

                    let next_sequence = match self.heap.peek() {
                        Some(next) => next.raw.sequence_number(),
                        None => return Poll::Pending,
                    };

                    let packet = if next_sequence == last.raw.sequence_number() + 1 {
                        match self.heap.pop() {
                            Some(packet) => packet.into(),
                            None => return Poll::Pending,
                        }
                    } else {
                        match (self.interpolation)(&last.raw, &self.heap.peek().unwrap().raw) {
                            Some(packet) => packet,
                            None => return Poll::Pending,
                        }
                    };

                    self.last = Some({
                        let mut yielded = JitterPacket::from(packet.clone());
                        yielded.yieleded_at = Some(SystemTime::now());
                        yielded
                    });

                    //println!(
                    //"yieleded after delay resolved: sn {}, heap size {}",
                    //packet.sequence_number(),
                    //self.heap.len()
                    //);

                    Poll::Ready(Some(packet))
                }
                Poll::Pending => Poll::Pending,
            },
            None => {
                let samples = last.raw.samples() / self.channels;
                let fraction = samples as f32 / self.sample_rate as f32;
                let elapsed = last
                    .yieleded_at
                    .unwrap_or_else(SystemTime::now)
                    .elapsed()
                    .unwrap_or(Duration::ZERO);
                let duration =
                    Duration::from_millis((fraction * 1000.0f32) as u64).saturating_sub(elapsed);

                self.delay = Some(Delay::new(duration));

                Poll::Pending
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.heap.len(), None)
    }
}

pub trait Packet: Unpin + Clone {
    fn sequence_number(&self) -> usize;
    fn offset(&self) -> usize;
    fn samples(&self) -> usize;
}

#[derive(Debug, Clone)]
pub(crate) struct JitterPacket<P>
where
    P: Packet,
{
    pub(crate) raw: P,
    pub(crate) yieleded_at: Option<SystemTime>,
}

impl<P> JitterPacket<P>
where
    P: Packet,
{
    fn into(self) -> P {
        self.raw
    }
}

impl<P> From<P> for JitterPacket<P>
where
    P: Packet,
{
    fn from(raw: P) -> Self {
        Self {
            raw,
            yieleded_at: None,
        }
    }
}

impl<P> PartialEq for JitterPacket<P>
where
    P: Packet,
{
    fn eq(&self, other: &Self) -> bool {
        self.raw.sequence_number().eq(&other.raw.sequence_number())
    }
}

impl<P> Eq for JitterPacket<P> where P: Packet {}

impl<P> PartialOrd for JitterPacket<P>
where
    P: Packet,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.raw
            .sequence_number()
            .partial_cmp(&other.raw.sequence_number())
            .map(|ordering| ordering.reverse())
    }
}

impl<P> Ord for JitterPacket<P>
where
    P: Packet,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.raw
            .sequence_number()
            .cmp(&other.raw.sequence_number())
            .reverse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::SinkExt;
    use futures::{executor::block_on, StreamExt};
    use std::time::SystemTime;

    const SAMPLE_RATE: usize = 48000;
    const CHANNELS: usize = 2;

    #[derive(Debug, Clone, PartialEq)]
    struct RTP {
        seq: usize,
        offset: usize,
    }

    impl Packet for RTP {
        #[inline]
        fn sequence_number(&self) -> usize {
            self.seq
        }

        #[inline]
        fn offset(&self) -> usize {
            self.offset
        }

        #[inline]
        fn samples(&self) -> usize {
            960
        }
    }

    #[test]
    fn const_capacity() {
        let jitter = JitterBuffer::<RTP, 10>::new(SAMPLE_RATE, CHANNELS);
        assert_eq!(jitter.heap.capacity(), 10);
    }

    #[test]
    fn send() {
        let mut jitter = JitterBuffer::<RTP, 10>::new(SAMPLE_RATE, CHANNELS);
        let packet = RTP { seq: 0, offset: 0 };
        block_on(jitter.send(packet.clone())).unwrap();
        assert_eq!(jitter.heap.peek(), Some(&packet.into()));
    }

    #[test]
    fn playback_according_to_sample_rate() {
        let mut jitter = JitterBuffer::<RTP, 10>::new(SAMPLE_RATE, CHANNELS);

        block_on(jitter.send(RTP { seq: 0, offset: 0 })).unwrap();
        block_on(jitter.send(RTP {
            seq: 1,
            offset: 960,
        }))
        .unwrap();
        block_on(jitter.send(RTP {
            seq: 2,
            offset: 960 * 2,
        }))
        .unwrap();

        assert_eq!(jitter.heap.len(), 3);
        assert!(jitter.last.is_none());

        let start = SystemTime::now();

        assert_eq!(block_on(jitter.next()), Some(RTP { seq: 0, offset: 0 }));
        assert_eq!(start.elapsed().unwrap().subsec_millis(), 0);
        assert_eq!(jitter.heap.len(), 2);
        assert_eq!(jitter.last.as_ref().unwrap().raw.sequence_number(), 0);
        assert_eq!(jitter.last.as_ref().unwrap().raw.offset(), 0);

        assert_eq!(
            block_on(jitter.next()),
            Some(RTP {
                seq: 1,
                offset: 960
            })
        );
        assert_eq!(start.elapsed().unwrap().subsec_millis(), 20);
        assert_eq!(jitter.heap.len(), 1);
        assert_eq!(jitter.last.as_ref().unwrap().raw.sequence_number(), 1);
        assert_eq!(jitter.last.as_ref().unwrap().raw.offset(), 960);

        assert_eq!(
            block_on(jitter.next()),
            Some(RTP {
                seq: 2,
                offset: 960 * 2
            })
        );
        assert_eq!(start.elapsed().unwrap().subsec_millis(), 40);

        assert_eq!(jitter.heap.len(), 0);
        assert_eq!(jitter.last.as_ref().unwrap().raw.sequence_number(), 2);
        assert_eq!(jitter.last.as_ref().unwrap().raw.offset(), 960 * 2);
    }

    #[test]
    fn reorders_racing_packets() {
        let mut jitter = JitterBuffer::<RTP, 10>::new(SAMPLE_RATE, CHANNELS);

        block_on(jitter.send(RTP { seq: 0, offset: 0 })).unwrap();
        assert_eq!(block_on(jitter.next()), Some(RTP { seq: 0, offset: 0 }));

        block_on(jitter.send(RTP {
            seq: 2,
            offset: 960 * 2,
        }))
        .unwrap();

        block_on(jitter.send(RTP {
            seq: 1,
            offset: 960,
        }))
        .unwrap();

        assert_eq!(
            block_on(jitter.next()),
            Some(RTP {
                seq: 1,
                offset: 960
            })
        );

        assert_eq!(
            block_on(jitter.next()),
            Some(RTP {
                seq: 2,
                offset: 960 * 2
            })
        );
    }

    #[test]
    fn discards_already_played_packets() {
        let mut jitter = JitterBuffer::<RTP, 10>::new(SAMPLE_RATE, CHANNELS);

        block_on(jitter.send(RTP { seq: 0, offset: 0 })).unwrap();
        assert_eq!(block_on(jitter.next()), Some(RTP { seq: 0, offset: 0 }));

        block_on(jitter.send(RTP { seq: 0, offset: 0 })).unwrap();

        block_on(jitter.send(RTP {
            seq: 1,
            offset: 960,
        }))
        .unwrap();
        assert_eq!(
            block_on(jitter.next()),
            Some(RTP {
                seq: 1,
                offset: 960
            })
        );
    }

    #[test]
    fn discards_duplicated_packets() {
        let mut jitter = JitterBuffer::<RTP, 10>::new(SAMPLE_RATE, CHANNELS);

        block_on(jitter.send(RTP { seq: 0, offset: 0 })).unwrap();
        block_on(jitter.send(RTP { seq: 0, offset: 0 })).unwrap();
        block_on(jitter.send(RTP { seq: 0, offset: 0 })).unwrap();
        block_on(jitter.send(RTP { seq: 0, offset: 0 })).unwrap();
        block_on(jitter.send(RTP { seq: 0, offset: 0 })).unwrap();

        assert_eq!(block_on(jitter.next()), Some(RTP { seq: 0, offset: 0 }));
        assert_eq!(jitter.heap.len(), 0);
    }
}
