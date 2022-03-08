"""
	This file just holds some configuration variables that will influence
	the behaviour of this library.
"""
conf = {
	# If set, we'll log each query we run
	"traceQueries": False,

	# A reference to the skeleton container of ViUR. Unless set, fetch() and getSkel() will fail
	"SkeletonInstanceRef": None,
}
