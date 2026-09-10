// Command promote-seed explicitly adds a reproduced randomized-test failure to
// that package's permanent regression corpus.
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/rivetdb/rivetdb/internal/testutil"
)

func main() {
	packageDir := flag.String("package", "", "package directory, for example ./internal/storage")
	testName := flag.String("test", "", "test name")
	seedText := flag.String("seed", "", "signed 64-bit seed")
	flag.Parse()

	if *packageDir == "" || *testName == "" || *seedText == "" {
		fmt.Fprintln(os.Stderr, "usage: promote-seed -package <dir> -test <name> -seed <int64>")
		os.Exit(2)
	}
	seed, err := strconv.ParseInt(*seedText, 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid seed %q: %v\n", *seedText, err)
		os.Exit(2)
	}

	path, err := testutil.PromoteSeed(*packageDir, *testName, seed)
	if err != nil {
		fmt.Fprintf(os.Stderr, "promote seed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("promoted seed %d to %s\n", seed, path)
}
