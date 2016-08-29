"""
Static check on a template.

* Operators that can only have 0 or 255 as its Y value
* Delayed replication descriptor must be followed by a delayed replication factor
* Number of descriptor included for nested replication descriptors make sense
* Operator descriptors follow certain pattern, e.g. 204 associated field etc.

"""