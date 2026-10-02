package main

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type Value struct {
	value     string
	expiresAt time.Time
}

type Storage struct {
	mu   sync.Mutex
	kv   map[string]*Value
	list map[string][]string
}

func (s *Storage) set(key string, value string) {
	s.mu.Lock()
	s.kv[key] = &Value{value: value}
	s.mu.Unlock()
}

func (s *Storage) get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, exists := s.kv[key]
	if exists && v.expiresAt.After(time.Now()) {
		return v.value, nil
	} else {
		return "", fmt.Errorf("Key doesn't exist.")
	}
}

func (s *Storage) setWithExpiration(key string, value string, arg string, interval int) {
	s.mu.Lock()
	switch strings.ToUpper(arg) {
	case "EX":
		s.kv[key] = &Value{value: value, expiresAt: time.Now().Add(time.Duration(interval) * time.Second)}
	case "PX":
		s.kv[key] = &Value{value: value, expiresAt: time.Now().Add(time.Duration(interval) * time.Millisecond)}
	}
	s.mu.Unlock()
}
