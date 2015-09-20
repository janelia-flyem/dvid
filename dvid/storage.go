package dvid

type StoreConfig struct {
	MetaData  EngineConfig
	Mutable   EngineConfig
	Immutable EngineConfig
}

type EngineConfig struct {
	Config

	// Engine is a simple name describing the engine, e.g., "basholeveldb"
	Engine string

	// Path can be a file path or URL stem
	Path string

	// Testing is true if this store is to be used for testing.
	Testing bool
}
