package transfer

import "golang.org/x/time/rate"

func rateNew(perSec int) *rate.Limiter {
	return rate.NewLimiter(rate.Limit(perSec), 8*1024)
}

type rateLimiter = rate.Limiter
