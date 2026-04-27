package cachefile

import (
	"encoding/json"

	"github.com/sagernet/bbolt"
	"github.com/sagernet/sing-box/adapter"
	E "github.com/sagernet/sing/common/exceptions"
)

var bucketTrafficStats = []byte("traffic_stats")

func (c *CacheFile) LoadTrafficStats(inboundTag string) (*adapter.TrafficStats, error) {
	var stats *adapter.TrafficStats
	err := c.view(func(tx *bbolt.Tx) error {
		bucket := c.bucket(tx, bucketTrafficStats)
		if bucket == nil {
			return nil
		}
		data := bucket.Get([]byte(inboundTag))
		if data == nil {
			return nil
		}
		stats = new(adapter.TrafficStats)
		return json.Unmarshal(data, stats)
	})
	if err != nil {
		return nil, err
	}
	return stats, nil
}

func (c *CacheFile) SaveTrafficStats(inboundTag string, stats *adapter.TrafficStats) error {
	return c.batch(func(tx *bbolt.Tx) error {
		bucket, err := c.createBucket(tx, bucketTrafficStats)
		if err != nil {
			return err
		}
		data, err := json.Marshal(stats)
		if err != nil {
			return E.Cause(err, "marshal traffic stats")
		}
		return bucket.Put([]byte(inboundTag), data)
	})
}
