package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/tryfix/log"
	"net/http"
)

type Err struct {
	Err string `json:"error"`
}

type handler struct {
	logger log.Logger
}

func (h *handler) encodeStores(w http.ResponseWriter, stores []Store) error {
	var list []string
	for _, stor := range stores {
		list = append(list, stor.Name())
	}
	return json.NewEncoder(w).Encode(list)
}

func (h *handler) encodeIndexes(w http.ResponseWriter, indexes []Index) error {
	var list []string
	for _, stor := range indexes {
		list = append(list, stor.String())
	}
	return json.NewEncoder(w).Encode(list)
}

func (h *handler) encodeAll(w http.ResponseWriter, i Iterator) error {

	var keyVals []struct {
		Key   interface{} `json:"key"`
		Value interface{} `json:"value"`
	}

	for i.Valid() {

		keyVal := struct {
			Key   interface{} `json:"key"`
			Value interface{} `json:"value"`
		}{}

		k, err := i.Key()
		if err != nil {
			h.logger.Error(err)
			i.Next()
			continue
		}

		v, err := i.Value()
		if err != nil {
			h.logger.Error(err)
			i.Next()
			continue
		}

		keyVal.Key = k
		keyVal.Value = v

		keyVals = append(keyVals, keyVal)
		i.Next()
	}

	return json.NewEncoder(w).Encode(keyVals)
}

func (h *handler) encodeItem(w http.ResponseWriter, k interface{}, v interface{}) error {

	keyVal := struct {
		Key   interface{} `json:"key"`
		Value interface{} `json:"value"`
	}{}

	keyVal.Key = k
	keyVal.Value = v

	return json.NewEncoder(w).Encode(keyVal)
}

func (h *handler) encodeError(e error) []byte {
	byt, err := json.Marshal(Err{
		Err: e.Error(),
	})
	if err != nil {
		h.logger.Error(err)
	}

	return byt
}

func (h *handler) storeExist(store string, registry Registry) bool {
	for _, s := range registry.Stores() {
		if s.Name() == store {
			return true
		}
	}

	return false
}

func (h *handler) indexExist(index string, registry Registry) bool {
	for _, idx := range registry.Indexes() {
		if idx.String() == index {
			return true
		}
	}

	return false
}

func MakeEndpoints(host string, registry Registry, logger log.Logger) {

	r := mux.NewRouter()
	h := handler{
		logger: logger,
	}

	r.HandleFunc(`/stores`, func(writer http.ResponseWriter, request *http.Request) {

		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Access-Control-Allow-Origin", "*")
		err := h.encodeStores(writer, registry.Stores())
		if err != nil {
			h.encodeError(err)
		}

	}).Methods(http.MethodGet)

	r.HandleFunc(`/indexes`, func(writer http.ResponseWriter, request *http.Request) {

		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Access-Control-Allow-Origin", "*")
		err := h.encodeIndexes(writer, registry.Indexes())
		if err != nil {
			h.encodeError(err)
		}

	}).Methods(http.MethodGet)

	r.HandleFunc(`/stores/{store}`, func(writer http.ResponseWriter, request *http.Request) {

		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Access-Control-Allow-Origin", "*")
		vars := mux.Vars(request)
		store, ok := vars[`store`]
		if !ok {
			logger.Error(`unknown route parameter`)
			return
		}

		if !h.storeExist(store, registry) {
			res := h.encodeError(errors.New(`store dose not exist`))
			if _, err := writer.Write(res); err != nil {
				logger.Error(err)
				return
			}
		}

		stor, err := registry.Store(store)
		if err != nil {
			res := h.encodeError(err)
			if _, err := writer.Write(res); err != nil {
				logger.Error(err)
				return
			}
			return
		}

		i, err := stor.GetAll(context.Background())
		if err != nil {
			res := h.encodeError(err)
			if _, err := writer.Write(res); err != nil {
				logger.Error(err)
				return
			}
		}

		err = h.encodeAll(writer, i)
		if err != nil {
			logger.Error(err)
		}

	}).Methods(http.MethodGet)

	r.HandleFunc(`/stores/{store}/indexes`, func(writer http.ResponseWriter, request *http.Request) {

		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Access-Control-Allow-Origin", "*")
		vars := mux.Vars(request)
		storeName, ok := vars[`store`]
		if !ok {
			logger.Error(`unknown route parameter`)
			return
		}

		if !h.storeExist(storeName, registry) {
			res := h.encodeError(errors.New(`index dose not exist`))
			if _, err := writer.Write(res); err != nil {
				logger.Error(err)
				return
			}
		}

		store, err := registry.Store(storeName)
		if err != nil {
			res := h.encodeError(err)
			if _, err := writer.Write(res); err != nil {
				logger.Error(err)
				return
			}
			return
		}

		indexdStore, ok := store.(IndexedStore)
		if !ok {
			res := h.encodeError(errors.New(`not an IndexedStore`))
			if _, err := writer.Write(res); err != nil {
				logger.Error(err)
				return
			}
		}

		keys := indexdStore.Indexes()
		var indexes []string
		for _, key := range keys {
			indexes = append(indexes, key.String())
		}

		err = json.NewEncoder(writer).Encode(indexes)
		if err != nil {
			logger.Error(err)
		}

	}).Methods(http.MethodGet)

	r.HandleFunc(`/stores/{store}/indexes/{index}`, func(writer http.ResponseWriter, request *http.Request) {

		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Access-Control-Allow-Origin", "*")
		vars := mux.Vars(request)
		storeName, ok := vars[`store`]
		if !ok {
			logger.Error(`unknown route parameter`)
			return
		}

		indexName, ok := vars[`index`]
		if !ok {
			logger.Error(`unknown route parameter`)
			return
		}

		if !h.storeExist(storeName, registry) {
			res := h.encodeError(errors.New(`store dose not exist`))
			if _, err := writer.Write(res); err != nil {
				logger.Error(err)
				return
			}
		}

		if !h.indexExist(indexName, registry) {
			res := h.encodeError(errors.New(`index dose not exist`))
			if _, err := writer.Write(res); err != nil {
				logger.Error(err)
				return
			}
		}

		store, err := registry.Store(storeName)
		if err != nil {
			res := h.encodeError(err)
			if _, err := writer.Write(res); err != nil {
				logger.Error(err)
				return
			}
			return
		}

		indexdStore, ok := store.(IndexedStore)
		if !ok {
			res := h.encodeError(errors.New(`not an IndexedStore`))
			if _, err := writer.Write(res); err != nil {
				logger.Error(err)
				return
			}
		}

		idx, err := indexdStore.GetIndex(request.Context(), indexName)
		if err != nil {
			res := h.encodeError(err)
			if _, err := writer.Write(res); err != nil {
				logger.Error(err)
				return
			}
			return
		}

		encoded := make(map[string][]interface{})

		for key, values := range idx.Values() {
			byt, err := json.Marshal(key)
			if err != nil {
				return
			}

			encoded[string(byt)] = values
		}

		err = json.NewEncoder(writer).Encode(encoded)
		if err != nil {
			logger.Error(err)
		}

	}).Methods(http.MethodGet)

	r.HandleFunc(`/stores/{store}/{key}`, func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")

		vars := mux.Vars(request)
		store, ok := vars[`store`]
		if !ok {
			logger.Error(`unknown route parameter`)
			return
		}

		if !h.storeExist(store, registry) {
			res := h.encodeError(errors.New(`store dose not exist`))
			if _, err := writer.Write(res); err != nil {
				logger.Error(err)
				return
			}
		}

		key, ok := vars[`key`]
		if !ok {
			logger.Error(`unknown route parameter`)
			return
		}

		keyByte := []byte(key)

		stor, err := registry.Store(store)
		if err != nil {
			res := h.encodeError(err)
			if _, err := writer.Write(res); err != nil {
				logger.Error(err)
				return
			}
			return
		}
		decodedKey, err := stor.KeyEncoder().Decode(keyByte)
		//@FIXME
		//keyInt, err := strconv.Atoi(key)
		if err != nil {
			return
		}

		data, err := stor.Get(context.Background(), decodedKey)
		if err != nil {
			res := h.encodeError(err)
			if _, err := writer.Write(res); err != nil {
				logger.Error(err)
				return
			}
		}

		err = h.encodeItem(writer, key, data)
		if err != nil {
			logger.Error(err)
		}

	}).Methods(http.MethodGet)

	go func() {
		err := http.ListenAndServe(host, handlers.CORS()(r))
		if err != nil {
			logger.Error(`k-stream.Store.Http`,
				fmt.Sprintf(`Cannot start web server : %+v`, err))
		}
	}()

	logger.Info(fmt.Sprintf(`Http server started on %s`, host))

}
