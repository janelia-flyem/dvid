package server

import (
	"io/ioutil"
	"os"
	"testing"
)

func loadConfigFile(t *testing.T, filename string) string {
	f, err := os.Open(filename)
	if err != nil {
		t.Fatalf("couldn't open TOML file %q: %v\n", filename, err)
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatalf("couldn't read TOML file %q: %v\n", filename, err)
	}
	return string(data)
}

func TestParseConfig(t *testing.T) {
	instanceCfg, logCfg, backendCfg, err := LoadConfig("../config-full.toml")
	if err != nil {
		t.Fatalf("bad TOML configuration: %v\n", err)
	}

	if instanceCfg.Gen != "sequential" || instanceCfg.Start != 100 {
		t.Errorf("Bad instance id retrieval of configuration: %v\n", instanceCfg)
	}

	if logCfg.Logfile != "/demo/logs/dvid.log" || logCfg.MaxSize != 500 || logCfg.MaxAge != 30 {
		t.Errorf("Bad logging configuration retrieval: %v\n", logCfg)
	}
	if backendCfg.DefaultKVDB != "raid6" || backendCfg.DefaultLog != "mutationlog" || backendCfg.KVStore["grayscale:99ef22cd85f143f58a623bd22aad0ef7"] != "kvautobus" {
		t.Errorf("Bad backend configuration retrieval: %v\n", backendCfg)
	}
}
