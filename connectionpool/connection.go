package connectionpool

// Connection is a small stand-in for a reusable network or database connection.
type Connection struct {
	id int
}

// ID returns the stable identifier assigned when the pool is created.
func (c *Connection) ID() int {
	return c.id
}
