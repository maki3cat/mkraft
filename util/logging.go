package util

import zap "go.uber.org/zap"

var logger *zap.Logger
var sugarLogger *zap.SugaredLogger

func InitLogger() {
	// Initialize the logger
	var err error
	logger, err = zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	sugarLogger = logger.Sugar()
	defer logger.Sync() // flushes buffer, if any
}

func GetLogger() *zap.Logger {
	return logger
}
func GetSugarLogger() *zap.SugaredLogger {
	return sugarLogger
}

// example usage of the logger
// logger, _ := zap.NewProduction()
// defer logger.Sync() // flushes buffer, if any
// sugar := logger.Sugar()
// sugar.Infow("failed to fetch URL",
//   // Structured context as loosely typed key-value pairs.
//   "url", url,
//   "attempt", 3,
//   "backoff", time.Second,
// )
// sugar.Infof("Failed to fetch URL: %s", url)
