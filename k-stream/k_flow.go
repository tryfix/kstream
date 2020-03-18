package kstream

//import (
//	"github.com/tryfix/kstream/errors"
//	"github.com/tryfix/kstream/flow"
//	"github.com/tryfix/kstream/logger"
//	"github.com/tryfix/kstream/processors"
//	"github.com/tryfix/kstream/source_sink"
//	"time"
//)
//
//type KFlow struct {
//	source           *kSource
//	sink             *kSink
//	branches         []*flow.Branch
//	processors       []processors.Processor
//	retryCount       int
//	retryInterval    time.Duration
//	changelogEnabled bool
//	errorHandler     errors.ErrorHandler
//}
//
//type KFlowBranchBuilder struct {
//	name       string
//	Builder    *kFlowBuilder
//	Predicate  flow.BranchPredicate
//	isParallel bool
//}
//
//type kFlowBuilder struct {
//	sourceBuilder    *kSourceBuilder
//	sinkBuilder      *kSinkBuilder
//	branches         []*KFlowBranchBuilder
//	processors       []processors.Processor
//	retryCount       int
//	retryInterval    time.Duration
//	changelogEnabled bool
//	errorHandler     errors.ErrorHandler
//}
//
//func (b *kFlowBuilder) Build() (flow.Flow, error) {
//
//	kFlow := &KFlow{
//		changelogEnabled: b.changelogEnabled,
//		errorHandler:     b.errorHandler,
//		processors:       b.processors,
//		retryCount:       b.retryCount,
//		retryInterval:    b.retryInterval,
//	}
//
//	if b.sourceBuilder != nil {
//		source, err := b.sourceBuilder.Build()
//		if err != nil {
//			return nil, err
//		}
//
//		kSource, ok := source.(*kSource)
//		if !ok {
//			logger.DefaultLogger.Fatal(`k-stream.kFlow`, `must be the type of kSource`)
//		}
//
//		kFlow.source = kSource
//	}
//
//	for _, branch := range b.branches {
//
//		// Build branch
//		flowBranch, err := branch.Builder.Build()
//		if err != nil {
//			logger.DefaultLogger.Fatal(`k-stream.kFlow`, err)
//		}
//
//		kFlow.branches = append(kFlow.branches, &flow.Branch{
//			Predicate: branch.Predicate,
//			Flow:      flowBranch,
//		})
//	}
//
//	if b.sinkBuilder != nil {
//		sink, err := b.sinkBuilder.Build()
//		if err != nil {
//			return nil, err
//		}
//
//		kSink, ok := sink.(*kSink)
//		if !ok {
//			logger.DefaultLogger.Fatal(`k-stream.kFlow`, `must be the type of kSource`)
//		}
//
//		kFlow.sink = kSink
//	}
//
//	return kFlow, nil
//}
//
//func (f *KFlow) Source() source_sink.Source {
//	return f.source
//}
//
//func (f *KFlow) Sink() source_sink.Sink {
//	return f.sink
//}
//
//func (f *KFlow) Processors() []processors.Processor {
//	return f.processors
//}
//
//func (f *KFlow) Branches() []*flow.Branch {
//	return f.branches
//}
//
//func (f *KFlow) Sinkable() bool {
//	return f.sink != nil
//}
//
//func (f *KFlow) OnError() errors.ErrorHandler {
//	return f.errorHandler
//}
//
//func (f *KFlow) RetryCount() int {
//	return f.retryCount
//}
//
//func (f *KFlow) RetryInterval() time.Duration {
//	return f.retryInterval
//}
