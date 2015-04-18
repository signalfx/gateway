package dpdimsort

import "sort"

// Ordering describes how dimensions should be sorted
type Ordering interface {
	Less(dimensions map[string]string, currentOrder []string, i int, j int) bool
	Sort(dimensions map[string]string) []string
}

type orderedOrdering struct {
	dimensionOrderMap map[string]int
}

func (comp *orderedOrdering) Less(_ map[string]string, currentOrder []string, i int, j int) bool {
	keyi := currentOrder[i]
	keyj := currentOrder[j]
	priority1, exist1 := comp.dimensionOrderMap[keyi]
	priority2, exist2 := comp.dimensionOrderMap[keyj]
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

// NewOrdering creates a sorter that sorts dimensions in dimensionsOrder order, putting
// any dimension inside dimensionsOrder ahead of any dimension not inside dimensionsOrder
func NewOrdering(dimensionsOrder []string) Ordering {
	dimensionOrderMap := make(map[string]int)
	for index, strVal := range dimensionsOrder {
		dimensionOrderMap[strVal] = index
	}
	return &orderedOrdering{
		dimensionOrderMap: dimensionOrderMap,
	}
}

// SortDimensions uses a Ordering to return the sorted order of a dimension map.  Useful for
// products that don't support dimension maps and require dimensions in a specific order
func (comp *orderedOrdering) Sort(dimensions map[string]string) []string {
	dims := make([]string, 0, len(dimensions))
	for k := range dimensions {
		dims = append(dims, k)
	}
	req := &orderedDimensionSortRequest{
		comparor:     comp,
		dimensions:   dimensions,
		currentOrder: dims,
	}
	sort.Sort(req)
	return req.currentOrder
}

type orderedDimensionSortRequest struct {
	comparor     Ordering
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
