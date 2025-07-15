package main

import (
	"context"
	"fmt"

	"github.com/YReshetko/pipeline"
)

func main() {
	p := pipeline.New([]int{1, 2, 3, 4})
	p = pipeline.Filter(p, func(_ context.Context, v int) (bool, error) { return v%2 == 0, nil })
	p = pipeline.Transformer(p, func(_ context.Context, v int) (int, error) {return v*2, nil})
	p = pipeline.Flatter(p, func(_ context.Context, v int) ([]int, error) {return []int{v, v}, nil})
	v, _ := p.Run(context.Background())
	fmt.Println(v)// 4 4 8 8 
}
