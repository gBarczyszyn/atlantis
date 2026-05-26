FROM gcr.io/oss-fuzz-base/base-builder-go@sha256:9f6114a73b4f3863dc8fa2afd6e7f61246c76f383cec701392759624fda1ebf2
COPY . $SRC/atlantis
COPY .clusterfuzzlite/build.sh $SRC/build.sh
WORKDIR $SRC/atlantis
