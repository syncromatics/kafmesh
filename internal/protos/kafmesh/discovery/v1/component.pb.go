// Code generated by protoc-gen-go. DO NOT EDIT.
// source: kafmesh/discovery/v1/component.proto

package discoveryv1

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

// Component is a part of a kafmesh service that completes a specific task.
type Component struct {
	Name                 string        `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Description          string        `protobuf:"bytes,2,opt,name=description,proto3" json:"description,omitempty"`
	Sources              []*Source     `protobuf:"bytes,3,rep,name=sources,proto3" json:"sources,omitempty"`
	Processors           []*Processor  `protobuf:"bytes,4,rep,name=processors,proto3" json:"processors,omitempty"`
	Sinks                []*Sink       `protobuf:"bytes,5,rep,name=sinks,proto3" json:"sinks,omitempty"`
	Views                []*View       `protobuf:"bytes,6,rep,name=views,proto3" json:"views,omitempty"`
	ViewSources          []*ViewSource `protobuf:"bytes,7,rep,name=view_sources,json=viewSources,proto3" json:"view_sources,omitempty"`
	ViewSinks            []*ViewSink   `protobuf:"bytes,8,rep,name=view_sinks,json=viewSinks,proto3" json:"view_sinks,omitempty"`
	XXX_NoUnkeyedLiteral struct{}      `json:"-"`
	XXX_unrecognized     []byte        `json:"-"`
	XXX_sizecache        int32         `json:"-"`
}

func (m *Component) Reset()         { *m = Component{} }
func (m *Component) String() string { return proto.CompactTextString(m) }
func (*Component) ProtoMessage()    {}
func (*Component) Descriptor() ([]byte, []int) {
	return fileDescriptor_ccfd3fa9b5aee317, []int{0}
}

func (m *Component) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Component.Unmarshal(m, b)
}
func (m *Component) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Component.Marshal(b, m, deterministic)
}
func (m *Component) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Component.Merge(m, src)
}
func (m *Component) XXX_Size() int {
	return xxx_messageInfo_Component.Size(m)
}
func (m *Component) XXX_DiscardUnknown() {
	xxx_messageInfo_Component.DiscardUnknown(m)
}

var xxx_messageInfo_Component proto.InternalMessageInfo

func (m *Component) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Component) GetDescription() string {
	if m != nil {
		return m.Description
	}
	return ""
}

func (m *Component) GetSources() []*Source {
	if m != nil {
		return m.Sources
	}
	return nil
}

func (m *Component) GetProcessors() []*Processor {
	if m != nil {
		return m.Processors
	}
	return nil
}

func (m *Component) GetSinks() []*Sink {
	if m != nil {
		return m.Sinks
	}
	return nil
}

func (m *Component) GetViews() []*View {
	if m != nil {
		return m.Views
	}
	return nil
}

func (m *Component) GetViewSources() []*ViewSource {
	if m != nil {
		return m.ViewSources
	}
	return nil
}

func (m *Component) GetViewSinks() []*ViewSink {
	if m != nil {
		return m.ViewSinks
	}
	return nil
}

func init() {
	proto.RegisterType((*Component)(nil), "kafmesh.discovery.v1.Component")
}

func init() {
	proto.RegisterFile("kafmesh/discovery/v1/component.proto", fileDescriptor_ccfd3fa9b5aee317)
}

var fileDescriptor_ccfd3fa9b5aee317 = []byte{
	// 334 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x8c, 0x92, 0x4f, 0x4b, 0x33, 0x31,
	0x10, 0x87, 0x69, 0xb7, 0x7f, 0xde, 0xce, 0xbe, 0x78, 0x08, 0x3d, 0x84, 0x22, 0x76, 0x15, 0x91,
	0x9e, 0xb6, 0xae, 0x82, 0x37, 0x11, 0xda, 0xde, 0x7a, 0x29, 0x2b, 0x14, 0xf1, 0x22, 0x75, 0x1b,
	0x31, 0x94, 0x4d, 0x96, 0x4c, 0x4d, 0xf1, 0xeb, 0xf4, 0xe8, 0xa7, 0x94, 0xcd, 0xa6, 0xa1, 0x87,
	0x74, 0xf1, 0x36, 0x6c, 0x9e, 0x67, 0x26, 0xf9, 0xcd, 0xc2, 0xf5, 0x66, 0xf5, 0x91, 0x33, 0xfc,
	0x1c, 0xaf, 0x39, 0x66, 0x52, 0x33, 0xf5, 0x3d, 0xd6, 0xc9, 0x38, 0x93, 0x79, 0x21, 0x05, 0x13,
	0xdb, 0xb8, 0x50, 0x72, 0x2b, 0x49, 0xdf, 0x52, 0xb1, 0xa3, 0x62, 0x9d, 0x0c, 0x2e, 0xbd, 0x2e,
	0xca, 0x2f, 0x95, 0xb1, 0x4a, 0x1c, 0xf8, 0xdb, 0x17, 0x4a, 0x66, 0x0c, 0x51, 0x2a, 0x4b, 0x0d,
	0xfd, 0x8d, 0xb8, 0xd8, 0xd4, 0x02, 0x9a, 0xb3, 0x9d, 0x05, 0x6e, 0x4e, 0x02, 0x6f, 0x7f, 0xb8,
	0x4f, 0xc5, 0xb9, 0x71, 0x57, 0xfb, 0x00, 0x7a, 0xd3, 0x43, 0x04, 0x84, 0x40, 0x4b, 0xac, 0x72,
	0x46, 0x1b, 0x51, 0x63, 0xd4, 0x4b, 0x4d, 0x4d, 0x22, 0x08, 0xd7, 0x0c, 0x33, 0xc5, 0x8b, 0x2d,
	0x97, 0x82, 0x36, 0xcd, 0xd1, 0xf1, 0x27, 0xf2, 0x00, 0xdd, 0x6a, 0x32, 0xd2, 0x20, 0x0a, 0x46,
	0xe1, 0xdd, 0x79, 0xec, 0x0b, 0x31, 0x7e, 0x36, 0x50, 0x7a, 0x80, 0xc9, 0x13, 0x80, 0x8b, 0x07,
	0x69, 0xcb, 0xa8, 0x43, 0xbf, 0xba, 0x38, 0x70, 0xe9, 0x91, 0x42, 0x6e, 0xa1, 0x5d, 0x3e, 0x05,
	0x69, 0xdb, 0xb8, 0x83, 0x13, 0x63, 0xb9, 0xd8, 0xa4, 0x15, 0x58, 0x1a, 0x65, 0x02, 0x48, 0x3b,
	0x75, 0xc6, 0x92, 0xb3, 0x5d, 0x5a, 0x81, 0x64, 0x0a, 0xff, 0x8f, 0xb2, 0x45, 0xda, 0x35, 0x62,
	0x74, 0x5a, 0xb4, 0xaf, 0x0c, 0xb5, 0xab, 0x91, 0x3c, 0x02, 0xb8, 0xe0, 0x91, 0xfe, 0x33, 0x2d,
	0x2e, 0x6a, 0x5a, 0x94, 0x37, 0xee, 0x69, 0x5b, 0xe1, 0x64, 0x09, 0x34, 0x93, 0xb9, 0x97, 0x9f,
	0x9c, 0xb9, 0xed, 0x2d, 0xca, 0x85, 0x2e, 0x1a, 0xaf, 0xa1, 0x3b, 0xd7, 0xc9, 0xbe, 0x19, 0xcc,
	0x67, 0x2f, 0x3f, 0xcd, 0xfe, 0xdc, 0xba, 0x33, 0xe7, 0x2e, 0x93, 0xf7, 0x8e, 0xf9, 0x07, 0xee,
	0x7f, 0x03, 0x00, 0x00, 0xff, 0xff, 0xe2, 0xe2, 0x17, 0x31, 0x1a, 0x03, 0x00, 0x00,
}
