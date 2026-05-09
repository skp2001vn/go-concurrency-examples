package connectionpool

// Connection represents one reusable connection that callers borrow from a Pool.
//
// In this example, the connection only has an ID so callers can tell which
// connection they received without depending on a real database or network.
type Connection struct {
	id int
}

// ID returns the connection's stable identifier.
func (c *Connection) ID() int {
	return c.id
}
