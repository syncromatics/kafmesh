// Code generated by MockGen. DO NOT EDIT.
// Source: ./processorLookup.go

// Package resolvers_test is a generated GoMock package.
package resolvers_test

import (
	gomock "github.com/golang/mock/gomock"
	model "github.com/syncromatics/kafmesh/internal/graph/model"
	reflect "reflect"
)

// MockProcessorLookupLoader is a mock of ProcessorLookupLoader interface
type MockProcessorLookupLoader struct {
	ctrl     *gomock.Controller
	recorder *MockProcessorLookupLoaderMockRecorder
}

// MockProcessorLookupLoaderMockRecorder is the mock recorder for MockProcessorLookupLoader
type MockProcessorLookupLoaderMockRecorder struct {
	mock *MockProcessorLookupLoader
}

// NewMockProcessorLookupLoader creates a new mock instance
func NewMockProcessorLookupLoader(ctrl *gomock.Controller) *MockProcessorLookupLoader {
	mock := &MockProcessorLookupLoader{ctrl: ctrl}
	mock.recorder = &MockProcessorLookupLoaderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockProcessorLookupLoader) EXPECT() *MockProcessorLookupLoaderMockRecorder {
	return m.recorder
}

// ProcessorByLookup mocks base method
func (m *MockProcessorLookupLoader) ProcessorByLookup(arg0 int) (*model.Processor, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ProcessorByLookup", arg0)
	ret0, _ := ret[0].(*model.Processor)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ProcessorByLookup indicates an expected call of ProcessorByLookup
func (mr *MockProcessorLookupLoaderMockRecorder) ProcessorByLookup(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ProcessorByLookup", reflect.TypeOf((*MockProcessorLookupLoader)(nil).ProcessorByLookup), arg0)
}

// TopicByLookup mocks base method
func (m *MockProcessorLookupLoader) TopicByLookup(arg0 int) (*model.Topic, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TopicByLookup", arg0)
	ret0, _ := ret[0].(*model.Topic)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// TopicByLookup indicates an expected call of TopicByLookup
func (mr *MockProcessorLookupLoaderMockRecorder) TopicByLookup(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TopicByLookup", reflect.TypeOf((*MockProcessorLookupLoader)(nil).TopicByLookup), arg0)
}
