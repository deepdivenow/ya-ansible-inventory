package common

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"
	"unsafe"
)

var (
	errEnv = errors.New("Env variable not set")
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func StringDump(data interface{}) string {
	b, _ := json.MarshalIndent(data, "", "  ")
	return string(b)
}
func StringClone(s string) string {
	b := make([]byte, len(s))
	copy(b, s)
	return *(*string)(unsafe.Pointer(&b))
}
func StringsClone(ss []string) []string {
	var rss []string
	for _, s := range ss {
		rss = append(rss, StringClone(s))
	}
	return rss
}

func FromBasicAuth(auth string) (username, password string, ok bool) {
	c, err := base64.StdEncoding.DecodeString(auth)
	if err != nil {
		return
	}
	cs := string(c)
	s := strings.IndexByte(cs, ':')
	if s < 0 {
		return
	}
	return cs[:s], cs[s+1:], true
}

func ToBasicAuth(username, password string) string {
	return base64.StdEncoding.EncodeToString([]byte(strings.Join([]string{username, password}, ":")))
}

func ToFront(data sort.Interface) (n int) {
	n = data.Len()
	if n == 0 {
		return
	}
	k := 0
	for i := 1; i < n; i++ {
		if data.Less(k, i) {
			k++
			data.Swap(k, i)
		}
	}
	return k + 1
}

// Contains tells whether a contains x.
func Contains(a []string, x string) bool {
	if len(a) < 1 {
		return false
	}
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

// generates a random string
func RandStr(min, max int, readable bool) string {

	var length int
	var char string

	if min < max {
		length = min + rand.Intn(max-min)
	} else {
		length = min
	}

	if readable == false {
		char = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	} else {
		char = "ABCDEFHJLMNQRTUVWXYZabcefghijkmnopqrtuvwxyz23479"
	}

	buf := make([]byte, length)
	for i := 0; i < length; i++ {
		buf[i] = char[rand.Intn(len(char)-1)]
	}
	return string(buf)
}

func MkDir(path string, perm os.FileMode) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err := os.Mkdir(path, perm)
		return err
	}
	return nil
}

func CheckEnvs(envLabels []string) (map[string]string, error) {
	envs := map[string]string{}
	for _, e := range envLabels {
		envValue := os.Getenv(e)
		if len(envValue) < 1 {
			return nil, errEnv
		}
		envs[e] = envValue
	}
	return envs, nil
}

// Given two maps, recursively merge right into left, NEVER replacing any key that already exists in left
func MergeKeys(left, right map[string]interface{}) map[string]interface{} {
	if left == nil {
		left = map[string]interface{}{}
	}
	if right == nil {
		return left
	}
	for key, rightVal := range right {
		if _, present := left[key]; !present {
			left[key] = rightVal
		}
	}
	return left
}

func RenameKeys(prefix string, in map[string]string) map[string]interface{} {
	out := map[string]interface{}{}
	for k, v := range in {
		out[prefix+k] = v
	}
	return out
}
