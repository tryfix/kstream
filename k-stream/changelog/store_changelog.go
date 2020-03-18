package changelog

func NewStoreChangelog(applicationId string, topic string, partition int32, opts ...Options) (Changelog, error) {

	options := new(options)
	options.apply(applicationId, opts...)

	return &stateChangelog{
		topic:         topic,
		partition:     partition,
		applicationId: applicationId,
		options:       options,
		//buffer:          NewBuffer(),
		changelogSuffix: `_store_changelog`,
	}, nil
}
