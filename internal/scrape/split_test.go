package scrape

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("splitComparison", func() {
	DescribeTable("splits a clean comparison",
		func(query, wantExpr, wantOp, wantLimit string) {
			expr, op, limit, ok := splitComparison(query)
			Expect(ok).To(BeTrue())
			Expect(expr).To(Equal(wantExpr))
			Expect(op).To(Equal(wantOp))
			Expect(limit).To(Equal(wantLimit))
		},
		Entry("gt", "up > 0.5", "up", "gt", "0.5"),
		Entry("ge", "sum(up) >= 3", "sum(up)", "ge", "3"),
		Entry("lt", "x < 1", "x", "lt", "1"),
		Entry("le", "x <= 2.5", "x", "le", "2.5"),
		Entry("eq", "x == 0", "x", "eq", "0"),
		Entry("ne", "x != 0", "x", "ne", "0"),
		Entry("comparison inside parens is not top-level",
			"histogram_quantile(0.99, rate(x[5m])) > 0.5",
			"histogram_quantile(0.99, rate(x[5m]))", "gt", "0.5"),
	)

	DescribeTable("does not split",
		func(query string) {
			_, _, _, ok := splitComparison(query)
			Expect(ok).To(BeFalse())
		},
		Entry("no comparison", "sum(up)"),
		Entry("non-numeric RHS", "a > b"),
		Entry("rhs is an expression", "x > 0.5 * y"),
		Entry("two top-level comparisons", "a > 1 < 2"),
		Entry("comparison only inside parens", "clamp_max(x, 5)"),
		Entry("empty lhs", "> 5"),
	)
})
