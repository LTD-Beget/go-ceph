package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// RateLimitSpec defines the specifications for rate limiting in requests.
// It includes fields for identifying the user and bucket, as well as settings for enabling/disabling limits
// and specifying read/write operations and byte limits.
type RateLimitSpec struct {
	UID           string `json:"uid" url:"uid"`
	Bucket        string `json:"bucket" url:"bucket"`
	Global        *bool  `json:"global" url:"global"`
	Scope         string `json:"ratelimit-scope" url:"ratelimit-scope"`
	Enabled       *bool  `json:"enabled,omitempty" url:"enabled"`
	MaxReadBytes  *int64 `json:"max_read_bytes" url:"max-read-bytes"`
	MaxWriteBytes *int64 `json:"max_write_bytes" url:"max-write-bytes"`
	MaxReadOps    *int64 `json:"max_read_ops" url:"max-read-ops"`
	MaxWriteOps   *int64 `json:"max_write_ops" url:"max-write-ops"`
}

// RateLimit represents the rate limit configuration in the response.
type RateLimit struct {
	MaxReadOps    *int64 `json:"max_read_ops"`
	MaxWriteOps   *int64 `json:"max_write_ops"`
	MaxReadBytes  *int64 `json:"max_read_bytes"`
	MaxWriteBytes *int64 `json:"max_write_bytes"`
	Enabled       *bool  `json:"enabled"`
}

// UserRateLimit represents the user-specific rate limit configuration in the response.
type UserRateLimit struct {
	UserRateLimit RateLimit `json:"user_ratelimit"`
}

// BucketRateLimit represents the bucket-specific rate limit configuration in the response.
type BucketRateLimit struct {
	BucketRateLimit RateLimit `json:"bucket_ratelimit"`
}

// GlobalRateLimit represents the global rate limit configuration in the response.
type GlobalRateLimit struct {
	BucketRateLimit    RateLimit `json:"bucket_ratelimit"`
	UserRateLimit      RateLimit `json:"user_ratelimit"`
	AnonymousRateLimit RateLimit `json:"anonymous_ratelimit"`
}

// GetUserRateLimit retrieves rate limits for a specific user
// https://docs.ceph.com/en/latest/radosgw/adminops/#get-user-rate-limit
func (api *API) GetUserRateLimit(ctx context.Context, ratelimit RateLimitSpec) (UserRateLimit, error) {
	if ratelimit.UID == "" {
		return UserRateLimit{}, errMissingUserID
	}

	ratelimit.Scope = "user"
	body, err := api.call(ctx, http.MethodGet, "/ratelimit", valueToURLParams(ratelimit, []string{"uid", "ratelimit-scope"}))
	if err != nil {
		return UserRateLimit{}, err
	}

	var ref UserRateLimit
	if err = json.Unmarshal(body, &ref); err != nil {
		return UserRateLimit{}, fmt.Errorf("%s. %s. %w", unmarshalError, string(body), err)
	}

	return ref, nil
}

// SetUserRateLimit sets rate limits for a specific user
// https://docs.ceph.com/en/latest/radosgw/adminops/#set-user-rate-limit
func (api *API) SetUserRateLimit(ctx context.Context, ratelimit RateLimitSpec) error {
	if ratelimit.UID == "" {
		return errMissingUserID
	}

	ratelimit.Scope = "user"
	_, err := api.call(ctx, http.MethodPost, "/ratelimit", valueToURLParams(ratelimit, []string{"uid", "ratelimit-scope", "enabled", "max-read-bytes", "max-write-bytes", "max-read-ops", "max-write-ops"}))
	return err
}
