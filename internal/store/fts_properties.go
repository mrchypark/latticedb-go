package store

func cloneFTSPropertiesShallow(properties map[string]StringPostings) map[string]StringPostings {
	if len(properties) == 0 {
		return nil
	}
	cloned := make(map[string]StringPostings, len(properties))
	for property, postings := range properties {
		cloned[property] = postings.Fork()
	}
	return cloned
}

func cloneStringPostingsDeep(postings StringPostings) StringPostings {
	cloned := NewStringPostings()
	for token := range postings.Keys() {
		for id := range postings.All(token) {
			cloned.Add(token, id)
		}
	}
	return cloned
}

func cloneFTSPropertiesDeep(properties map[string]StringPostings) map[string]StringPostings {
	if len(properties) == 0 {
		return nil
	}
	cloned := make(map[string]StringPostings, len(properties))
	for property, postings := range properties {
		cloned[property] = cloneStringPostingsDeep(postings)
	}
	return cloned
}
