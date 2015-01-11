package sorting

import "sort"

// DimensionComparor describes how dimensions should be sorted
type DimensionComparor interface {
	Less(dimensions map[string]string, currentOrder []string, i int, j int) bool
}

type orderedDimensionComparor struct {
	dimensionOrderMap map[string]int
}

func (comparor *orderedDimensionComparor) Less(_ map[string]string, currentOrder []string, i int, j int) bool {
	keyi := currentOrder[i]
	keyj := currentOrder[j]
	priority1, exist1 := comparor.dimensionOrderMap[keyi]
	priority2, exist2 := comparor.dimensionOrderMap[keyj]
	if exist1 && !exist2 {
		return true
	}
	if !exist1 && exist2 {
		return false
	}
	if exist1 && exist2 && priority1 != priority2 {
		return priority1 < priority2
	}

	return keyi < keyj
}

// NewOrderedDimensionComparor creates a sorter that sorts dimensions in dimensionsOrder order
func NewOrderedDimensionComparor(dimensionsOrder []string) DimensionComparor {
	dimensionOrderMap := make(map[string]int)
	for index, strVal := range dimensionsOrder {
		dimensionOrderMap[strVal] = index
	}
	return &orderedDimensionComparor{
		dimensionOrderMap: dimensionOrderMap,
	}
}

// SortDimensions uses a DimensionComparor to return the sorted order of a dimension map
func SortDimensions(comparor DimensionComparor, dimensions map[string]string) []string {
	dims := make([]string, 0, len(dimensions))
	for k := range dimensions {
		dims = append(dims, k)
	}
	req := &orderedDimensionSortRequest{
		comparor:     comparor,
		dimensions:   dimensions,
		currentOrder: dims,
	}
	sort.Sort(req)
	return req.currentOrder
}

type orderedDimensionSortRequest struct {
	comparor     DimensionComparor
	currentOrder []string
	dimensions   map[string]string
}

func (sortRequest *orderedDimensionSortRequest) Len() int {
	return len(sortRequest.currentOrder)
}

func (sortRequest *orderedDimensionSortRequest) Swap(i, j int) {
	sortRequest.currentOrder[i], sortRequest.currentOrder[j] = sortRequest.currentOrder[j], sortRequest.currentOrder[i]
}

func (sortRequest *orderedDimensionSortRequest) Less(i, j int) bool {
	return sortRequest.comparor.Less(sortRequest.dimensions, sortRequest.currentOrder, i, j)
}
