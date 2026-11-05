#!/busybox/sh
set -eu
if [ "${S3_INTEGRATION_TEST:-}" != 1 ]; then
  echo 'Set S3_INTEGRATION_TEST=1 to run tests against the configured S3 backend.' >&2
  exit 1
fi
exec /s3-integration.test "$@"
