// deps-check 检查 services/、libs/、plugins/ 之间的依赖是否符合技术设计文档 12.2 的依赖规则。
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	root := flag.String("root", ".", "仓库根目录")
	flag.Parse()
	vs, err := check(os.DirFS(*root))
	if err != nil {
		fmt.Fprintln(os.Stderr, "deps-check:", err)
		os.Exit(2)
	}
	for _, v := range vs {
		fmt.Println(v)
	}
	if len(vs) > 0 {
		fmt.Fprintf(os.Stderr, "deps-check: %d 处违反依赖规则（技术设计文档 12.2）\n", len(vs))
		os.Exit(1)
	}
}
