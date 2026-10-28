# saramaskip

Sarama consumer handler with endless retries and runtime bypass expressions.

## Usage

```go
handler, err := saramaskip.New(
	"orders-worker",
	func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		return process(ctx, msg)
	},
	saramaskip.WithRetryInterval(2*time.Second),
)
```

Pass `handler` to `consumer.Consume`. Failed messages retry until they succeed or match a bypass expression. Only successful or bypassed messages are marked.

To reload an expression whenever a file changes:

```go
go handler.WatchBypassExpressionFile(
	ctx,
	"examples/bypass.expression",
	time.Second,
	func(err error) { log.Println(err) },
)
```

Read and parse errors keep the last valid expression. You can also set it directly:

```go
err := handler.SetBypassExpression(
	`topic == "orders" and partition == 3 and offset == 12042`,
)
```

Fields: `topic`, `group`, `error`, `partition`, `offset`, `attempt`.  
Operators: `and`, `or`, `not`, `==`, `!=`, `<`, `<=`, `>`, `>=`, `contains`.

An empty expression disables bypass. An invalid update preserves the previous expression.
