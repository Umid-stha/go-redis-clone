package main

import (
	"fmt"
	"slices"
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
	if exists && (v.expiresAt.After(time.Now()) || v.expiresAt.IsZero()) {
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

func (s *Storage) rpush(key string, elements []string) int {
	s.mu.Lock()
	s.list[key] = append(s.list[key], elements...)
	s.mu.Unlock()
	return len(s.list[key])
}

func (s *Storage) lpush(key string, elements []string) int {
	s.mu.Lock()
	slices.Reverse(elements)
	s.list[key] = append(elements, s.list[key]...)
	s.mu.Unlock()
	return len(s.list[key])
}

func (s *Storage) lrange(key string, start int, end int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	list, exists := s.list[key]
	length := len(list)
	if !exists {
		return []string{}
	}
	if start >= length {
		return []string{}
	}
	if start < 0 {
		if -start > length {
			start = 0
		} else {
			start = length + start
		}
	}
	if end < 0 {
		if -end > length {
			end = 0
		} else {
			end = length + end
		}
	}
	if end >= length {
		end = length - 1
	}
	return list[start : end+1]
}

func (s *Storage) lpop(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list, exists := s.list[key]
	if !exists {
		return "", fmt.Errorf("Doesn't exist")
	}
	if len(list) == 0 {
		return "", fmt.Errorf("Empty")
	}
	t_element := list[0]
	copy(list, list[1:])
	//Drop the last empty element
	s.list[key] = list[:len(list)-1]
	return t_element, nil
}

func (s *Storage) llen(key string) int {
	list, exists := s.list[key]
	if !exists {
		return 0
	}
	return len(list)
}
