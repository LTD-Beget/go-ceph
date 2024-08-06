package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// GetBucketRateLimit retrieves rate limits for a specific bucket
// https://docs.ceph.com/en/latest/radosgw/adminops/#get-bucket-rate-limit
func (api *API) GetBucketRateLimit(ctx context.Context, ratelimit RateLimitSpec) (BucketRateLimit, error) {
	if ratelimit.Bucket == "" {
		return BucketRateLimit{}, errMissingBucket
	}

	ratelimit.Scope = "bucket"
	body, err := api.call(ctx, http.MethodGet, "/ratelimit", valueToURLParams(ratelimit, []string{"bucket", "ratelimit-scope"}))
	if err != nil {
		return BucketRateLimit{}, err
	}

	var ref BucketRateLimit
	if err = json.Unmarshal(body, &ref); err != nil {
		return BucketRateLimit{}, fmt.Errorf("%s. %s. %w", unmarshalError, string(body), err)
	}

	return ref, nil
}

// SetBucketRateLimit sets rate limits for a specific bucket
// https://docs.ceph.com/en/latest/radosgw/adminops/#set-rate-limit-for-an-individual-bucket
func (api *API) SetBucketRateLimit(ctx context.Context, ratelimit RateLimitSpec) error {
	if ratelimit.Bucket == "" {
		return errMissingBucket
	}

	ratelimit.Scope = "bucket"
	_, err := api.call(ctx, http.MethodPost, "/ratelimit", valueToURLParams(ratelimit, []string{"bucket", "ratelimit-scope", "enabled", "max-read-bytes", "max-write-bytes", "max-read-ops", "max-write-ops"}))
	return err
}
