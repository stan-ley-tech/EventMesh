import sys

from eventmesh import Client, ConsumerGroupWorker
from order_events import GROUP


def main():
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
    worker_id = sys.argv[2] if len(sys.argv) > 2 else "worker-1"

    client = Client(base_url)
    worker = ConsumerGroupWorker(client, GROUP, worker_id=worker_id, poll_interval=0.5, heartbeat_interval=5)

    @worker.handler
    def handle(event, ctx):
        print(f"[{worker_id}] processing {event.payload['order_id']} (attempt {ctx.attempt}, partition {event.partition})", flush=True)

    print(f"[{worker_id}] polling {base_url} for group {GROUP!r}", flush=True)
    worker.run()


if __name__ == "__main__":
    main()
