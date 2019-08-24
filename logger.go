package migrate

import "fmt"

type Logger interface {
	Printf(string, ...interface{})
	Println(...interface{})
}

// StdLogger is a helper type that simply logs to stdout using fmt. Unless you
// want to structure logs or redirect them in some way, this is probably what
// you want to use in migrate.New().
type StdLogger struct{}

func (l StdLogger) Printf(s string, vs ...interface{}) {
	fmt.Printf(s, vs...)
}

func (l StdLogger) Println(vs ...interface{}) {
	fmt.Println(vs...)
}
