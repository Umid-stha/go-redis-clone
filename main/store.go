package main

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

type ValueType int

const (
	TypeString ValueType = iota
	TypeList
	TypeStream
)

type Value struct {
	Type      ValueType
	Value     any
	ExpiresAt time.Time
}

type Storage struct {
	mu         sync.Mutex
	kv         map[string]*Value
	blockQueue map[string][]chan string
}

func (s *Storage) getListValue(key string) ([]string, error) {
	s.mu.Lock()
	data, exists := s.kv[key]
	s.mu.Unlock()
	if !exists {
		return []string{}, fmt.Errorf("Doesn't exist")
	}
	if exists && data.Type != TypeList {
		return []string{}, ErrWrongType
	}
	return data.Value.([]string), nil
}

func (s *Storage) setListValue(key string, newList []string) error {
	s.mu.Lock()
	data, exists := s.kv[key]
	if exists && data.Type != TypeList {
		return ErrWrongType
	}
	if !exists {
		s.kv[key].Type = TypeList
	}
	s.kv[key].Value = newList
	s.mu.Unlock()
	return nil
}

func (s *Storage) set(key string, value string) {
	s.mu.Lock()
	s.kv[key] = &Value{Type: TypeString, Value: value}
	s.mu.Unlock()
}

func (s *Storage) get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, exists := s.kv[key]
	if v.Type != TypeString {
		return "", ErrWrongType
	}
	if !exists {
		return "", fmt.Errorf("Key doesn't exist.")
	}
	if time.Now().After(v.ExpiresAt) && !v.ExpiresAt.IsZero() {
		delete(s.kv, key)
		return "", fmt.Errorf("Key doesn't exist.")
	}
	return v.Value.(string), nil
}

func (s *Storage) setWithExpiration(key string, value string, arg string, interval int) {
	s.mu.Lock()
	switch strings.ToUpper(arg) {
	case "EX":
		s.kv[key] = &Value{Type: TypeString, Value: value, ExpiresAt: time.Now().Add(time.Duration(interval) * time.Second)}
	case "PX":
		s.kv[key] = &Value{Type: TypeString, Value: value, ExpiresAt: time.Now().Add(time.Duration(interval) * time.Millisecond)}
	}
	s.mu.Unlock()
}

func (s *Storage) rpush(key string, elements []string) (int, error) {
	list, err := s.getListValue(key)
	if err != nil {
		return 0, err
	}
	list = append(list, elements...)
	err = s.setListValue(key, list)
	return len(list), nil
}

func (s *Storage) lpush(key string, elements []string) (int, error) {
	slices.Reverse(elements)
	list, err := s.getListValue(key)
	if err != nil {
		return 0, err
	}
	list = append(elements, list...)
	err = s.setListValue(key, list)
	return len(list), nil
}

func (s *Storage) lrange(key string, start int, end int) ([]string, error) {
	list, err := s.getListValue(key)
	if err != nil {
		return []string{}, err
	}
	length := len(list)
	if start >= length {
		return []string{}, nil
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
	return list[start : end+1], nil
}

func (s *Storage) ipop(key string, index int) (string, error) {
	list, err := s.getListValue(key)
	if err != nil {
		return "", err
	}
	if len(list) == 0 {
		return "", fmt.Errorf("Empty")
	}
	if index >= len(list) {
		return "", fmt.Errorf("Index out of bound")
	}
	t_element := list[index]
	copy(list[:index], list[index+1:])
	//Drop the last empty element
	err = s.setListValue(key, list[:len(list)-1])
	if err != nil {
		return "", err
	}
	return t_element, nil
}

func (s *Storage) lpop(key string) (string, error) {
	list, err := s.getListValue(key)
	if err != nil {
		return "", err
	}
	if len(list) == 0 {
		return "", fmt.Errorf("Empty")
	}
	t_element := list[0]
	copy(list, list[1:])
	//Drop the last empty element
	err = s.setListValue(key, list[:len(list)-1])
	if err != nil {
		return "", err
	}
	return t_element, nil
}

func (s *Storage) mLpop(key string, num int) ([]string, error) {
	list, err := s.getListValue(key)
	if err != nil {
		return []string{}, err
	}
	if len(list) == 0 {
		return []string{}, fmt.Errorf("Empty")
	}
	if num > len(list) {
		num = len(list)
	}
	t_elements := make([]string, num)
	copy(t_elements, list[:num])
	copy(list, list[num:])
	//Drop the last empty element
	err = s.setListValue(key, list[:len(list)-1])
	if err != nil {
		return []string{}, err
	}
	return t_elements, nil
}

func (s *Storage) llen(key string) (int, error) {
	list, err := s.getListValue(key)
	if err != nil {
		return 0, err
	}
	return len(list), nil
}
