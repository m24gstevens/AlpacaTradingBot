import asyncio
import websockets
import msgpack
from collections import defaultdict
from queue import Queue

class DataStream:
    def __init__(self,endpoint,api_key,secret_key):
        self._endpoint = endpoint
        self._api_key = api_key
        self._secret_key = secret_key
        self._ws = None
        self._websocket_params = {
            "ping_interval": 10,
            "ping_timeout": 200,
            "max_queue": 1024
        }
        self._handlers = {
            "trades":{},
            "quotes":{},
            "bars":{}
        }
        self._stop_queue = Queue()
        self._is_running = False
        self._should_run = True
        self._max_frame_size = 32768
    
    async def _auth(self):
        creds = msgpack.packb({
            "action":"auth",
            "key":self._api_key,
            "secret":self._secret_key
        })
        await self._ws.send(creds)

        r = await self._ws.recv()
        msg = msgpack.unpackb(r)
        print(msg)
        if msg[0]["T"] == "error":
            raise ValueError(msg[0].get("msg", "auth failed"))
        if msg[0]["T"] != "success" or msg[0]["msg"] != "authenticated":
            raise ValueError("failed to authenticate")

    async def _start_up(self):
        await self._connect()
        await self._auth()

    async def _connect(self):
        self._ws = await websockets.connect(self._endpoint,
        extra_headers={"Content-Type":"application/msgpack"},
        **self._websocket_params)

        r = await self._ws.recv()
        msg = msgpack.unpackb(r)
        print(msg)
        if msg[0]["T"] != "success" or msg[0]["msg"] != "connected":
            raise ValueError("connected message not received")

    async def _close(self):
        if self._ws:
            self._ws.close()
            self._ws = None
            self._is_running = False

    async def _signal_stop(self):
        self._should_run = False
        if self._stop_queue.empty():
            self._stop_queue.put_nowait({"should_stop":True})

    async def _listen(self):
        while True:
            if not self._stop_queue.empty():
                self._stop_queue.get(timeout=1)
                await self._close()
                break
            else:
                try:
                    r = await asyncio.wait_for(self._ws.recv(),5)
                    msgs = msgpack.unpackb(r)
                    for msg in msgs:
                        await self._dispatch(msg)
                except asyncio.TimeoutError:
                    pass

    async def _dispatch(self,msg):
        msg_type = msg.get("T")
        symbol = msg.get("S")
        if msg_type in ['t','q','b']:
            handle_group = {'t':"trades", 'q':"quotes", 'b':"bars"}[msg_type]
            handler = self._handlers[handle_group].get(
                symbol, self._handlers[handle_group].get("*", None)
            )
            if handler:
                await handler(msg)

        elif msg_type == "subscription":
            sub = [f"{k}: {msg.get(k, [])}" for k in self._handlers]
            print(f"subscribed to {', '.join(sub)}")
        elif msg_type == "error":
            print(f"error: {msg.get('msg')} ({msg.get('code')})")

    def _subscribe(self,handler, symbol, handlers):
        handlers[symbol] = handler
        if self._is_running:
            asyncio.run_coroutine_threadsafe(self._send_subscriptions(), self._loop).result()

    async def _send_subcriptions(self):
        msg = defaultdict(list)
        for k, v in self._handlers.items():
            if k not in ("cancelErrors", "corrections") and v:
                for s in v.keys():
                    msg[k].append(s)
        msg["action"] = "subscribe"
        bin = msgpack.packb(msg)
        frames = (bin[i:i+self._max_frame_size] for i in range(0,len(bin),self._max_frame_size))
        await self._ws.send(frames)

    async def _unsubscribe(self, trades=(), quotes=(), bars=()):
        if trades or quotes or bars:
            await self._ws.send(
                msgpack.packb({
                    "action": "unsubscribe",
                    "trades": trades,
                    "quotes": quotes,
                    "bars": bars
                })
            )


    async def _do_run(self):
        self._loop = asyncio.get_running_loop()
        # must subscribe first
        while not any(
            v for k,v in self._handlers.items()
            if k not in ("cancelErrors", "corrections")
        ):
            if not self._stop_queue.empty():
                self._stop_queue.get(timeout=1)
                return
            await asyncio.sleep(0)
        print("started stream")
        self._should_run = True
        self._running = False
        while True:
            try:
                if not self._should_run:
                    print("stream stopped")
                    return
                if not self._is_running:
                    print("starting a websocket connection")
                    await self._start_up()
                    await self._send_subcriptions()
                    self._is_running = True
                await self._listen()
            
            except websockets.WebSocketException as wse:
                await self._close()
                self._running = False
                print("Websocket error, restarting connection")
            #except Exception as e:
                #print(f"error during websocket communication: {str(e)}")
            finally:
                await asyncio.sleep(0)



    def subscribe_trades(self, handler, symbol):
        self._subscribe(handler, symbol, self._handlers["trades"])

    def subscribe_quotes(self, handler, symbol):
        self._subscribe(handler, symbol, self._handlers["quotes"])

    def subscribe_bars(self, handler, symbol):
        self._subscribe(handler, symbol, self._handlers["bars"])

    def unsubscribe_trades(self, *symbols):
        if self._running:
            asyncio.run_coroutine_threadsafe(
                self._unsubscribe(trades=symbols), self._loop
            ).result()
        for symbol in symbols:
            del self._handlers["trades"][symbol]
    
    def unsubscribe_quotes(self, *symbols):
        if self._running:
            asyncio.run_coroutine_threadsafe(
                self._unsubscribe(quotes=symbols), self._loop
            ).result()
        for symbol in symbols:
            del self._handlers["quotes"][symbol]

    def unsubscribe_bars(self, *symbols):
        if self._running:
            asyncio.run_coroutine_threadsafe(
                self._unsubscribe(bars=symbols), self._loop
            ).result()
        for symbol in symbols:
            del self._handlers["bars"][symbol]

    def stop(self) -> None:
        """Stops the websocket connection."""
        if self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self._signal_stop(), self._loop).result()

    def run(self):
        try:
            asyncio.run(self._do_run())
        except KeyboardInterrupt:
            print("keyboard interrupt, bye")
            self._ws.close()
            pass



"""the_data = DataStream('wss://stream.data.alpaca.markets/v1beta2/crypto',API_KEY,API_SECRET)

async def simple_callback(msg):
    print(msg)

the_data.subscribe_bars(simple_callback, "BTC/USD")
the_data.run()"""