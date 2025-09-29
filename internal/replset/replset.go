package replset

type Replset struct {
	ports []string
}

func NewReplset(ports []string) *Replset {
	return &Replset{
		ports: ports,
	}
}
