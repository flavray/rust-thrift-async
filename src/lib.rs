#[macro_use]
extern crate futures;
extern crate thrift;
extern crate tokio;

mod transport;

use std::net::SocketAddr;
use std::sync::Arc;

use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::server::TProcessor;
use tokio::net::TcpListener;
use tokio::prelude::*;

use transport::{ReadResult, TAReadFramedTransport, TAWriteFramedTransport};

pub const DEFAULT_MAX_FRAME_SIZE: u32 = 256 * 1024 * 1024;

struct TAsyncProcessor<R: AsyncRead, W: AsyncWrite, P: TProcessor> {
    reader: R,
    writer: W,
    processor: Arc<P>,
    max_frame_size: usize,
}

impl<R: AsyncRead, W: AsyncWrite, P: TProcessor> TAsyncProcessor<R, W, P> {
    fn new(reader: R, writer: W, processor: Arc<P>, max_frame_size: usize) -> Self {
        TAsyncProcessor {
            reader,
            writer,
            processor,
            max_frame_size,
        }
    }
}

impl<R: AsyncRead, W: AsyncWrite, P: TProcessor> Future for TAsyncProcessor<R, W, P> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {
        let max_frame_size = self.max_frame_size as usize;
        let mut read_transport = TAReadFramedTransport::new(&mut self.reader, max_frame_size);

        let mut write_transport = TAWriteFramedTransport::new(&mut self.writer);

        loop {
            match try_ready!(read_transport.poll()) {
                ReadResult::Ok => (),
                ReadResult::EOF => break, // EOF reached - client disconnected
                ReadResult::TooLarge(_) => break, // Frame is too large (non-thrift framed input?) - disconnect client  // TODO: log error
            }

            let read_cursor = read_transport.frame_cursor();

            {
                // TODO: I want NLL :(
                let write_cursor = write_transport.frame_cursor();

                let mut input_protocol = TBinaryInputProtocol::new(read_cursor, false);
                let mut output_protocol = TBinaryOutputProtocol::new(write_cursor, false);

                if let Err(e) = self
                    .processor
                    .process(&mut input_protocol, &mut output_protocol)
                {
                    println!("Error {:?}", e);
                }
            }

            try_ready!(write_transport.poll());
        }

        Ok(Async::Ready(()))
    }
}

pub struct TAsyncServer<P: TProcessor + Send + Sync + 'static> {
    processor: Arc<P>,
    max_frame_size: u32,
}

impl<P: TProcessor + Send + Sync + 'static> TAsyncServer<P> {
    pub fn new(processor: P) -> TAsyncServer<P> {
        TAsyncServer {
            processor: Arc::new(processor),
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }

    pub fn max_frame_size(&mut self, max_frame_size: u32) -> &mut Self {
        self.max_frame_size = max_frame_size;
        self
    }

    pub fn listen(&mut self, address: &str) {
        let address = address.parse::<SocketAddr>().unwrap(); // TODO: error

        self.listen_address(address)
    }

    pub fn listen_address(&mut self, address: SocketAddr) {
        let socket = TcpListener::bind(&address).unwrap(); // TODO: error

        let processor = self.processor.clone();
        let max_frame_size = self.max_frame_size as usize;

        let server = socket
            .incoming()
            .map_err(|err| println!("Socket Error: {:?}", err)) // TODO: error
            .for_each(move |socket| {
                let (reader, writer) = socket.split();
                let processor = processor.clone();
                let async_processor =
                    TAsyncProcessor::new(reader, writer, processor, max_frame_size);

                tokio::spawn(async_processor)
            });

        tokio::run(server);

        /*
        TODO: one day.
        self.runtime
            .spawn(server);
        
        self.runtime
            .shutdown_on_idle()
            .wait()
            .unwrap();  // TODO: error
        */
    }
}
