package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"
)

type CacheItem struct {
	Value     interface{}
	Timestamp int64
}

type Cache struct {
	sync.RWMutex
	data        map[string]*CacheItem
	serverURL   string
	cacheTTL    time.Duration
	client      *http.Client
}

func NewCache(serverURL string, cacheTTL time.Duration) *Cache {
	return &Cache{
		data:      make(map[string]*CacheItem),
		serverURL: serverURL,
		cacheTTL:  cacheTTL,
		client:    &http.Client{},
	}
}

func (c *Cache) Get(collectionName, key string) (interface{}, error) {
	c.RLock()
	item, exists := c.data[key]
	c.RUnlock()

	if exists && time.Now().Unix()-item.Timestamp < int64(c.cacheTTL.Seconds()) {
		return item.Value, nil
	}

	value, err := c.fetch(collectionName, key)
	if err != nil {
		return nil, err
	}

	c.Lock()
	defer c.Unlock()
	c.data[key] = &CacheItem{
		Value:     value,
		Timestamp: time.Now().Unix(),
	}
	return value, nil
}

func (c *Cache) fetch(collectionName, key string) (interface{}, error) {
	resp, err := c.client.Get(c.serverURL + "/collections/" + collectionName + "?value=" + key)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var value interface{}
	err = json.Unmarshal(body, &value)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func main() {
	serverURL := "http://localhost:8080"
	cache := NewCache(serverURL, 5*time.Minute)

	http.HandleFunc("/collections/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "invalid method", http.StatusMethodNotAllowed)
			return
		}

		collectionName := r.URL.Path[len("/collections/"):]
		key := r.URL.Query().Get("value")

		value, err := cache.Get(collectionName, key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		json.NewEncoder(w).Encode(value)
	})

	log.Fatal(http.ListenAndServe(":8081", nil))
}
