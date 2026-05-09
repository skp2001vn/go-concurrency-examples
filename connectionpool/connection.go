package connectionpool

// Connection is a reusable resource managed by a Pool.
//
// In this example, a Connection only carries a stable ID so tests can verify
// acquisition and release behavior without depending on a real network or
// database connection.
type Connection struct {
	id int
}

// ID returns the stable identifier assigned to c when its Pool is created.
func (c *Connection) ID() int {
	return c.id
}
