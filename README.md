# Go Parquet Benchmarks

## To run the benchmarks
1. [Download](https://go.dev/dl/) and [install](https://go.dev/doc/install) Go.
2. Git clone this repository: `git clone https://github.com/drabastomek/parquet-go-benchmarks`
3. Run 
    ```bash
    go mod download
    go mod verify
    ```
    to make sure all the dependencies are downloaded into local cache and verified.
3. Run the benchmark: 
    ```bash
    cd parquet-benchmarks
    go test -bench . ./writer/
    go test -bench . ./reader/
    ```
