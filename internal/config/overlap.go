// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"sort"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/openconfig/gnmic/pkg/api/path"
)

// Oversubscription validation: reject a profile set whose combined paths make a
// device stream the same leaf more than once — either the same path twice, or a
// parent container plus one of its own leaves. Paths are parsed with the same
// gnmic parser the subscribe builder uses (api.Path), so a path that fails here
// would also fail at subscribe time and vice versa. Keys are matched with
// wildcard semantics: an absent key or "*" on either side intersects any value.

type profilePath struct {
	profile string
	raw     string
	elems   []*gnmipb.PathElem
}

// validateNoOverlap checks the union of all profiles' paths pairwise. All hosts
// subscribe to all profiles, so the union is the per-device subscription set.
func validateNoOverlap(profiles map[string]SubscriptionProfile) error {
	names := make([]string, 0, len(profiles))
	for name := range profiles {
		names = append(names, name)
	}
	sort.Strings(names) // deterministic error output

	var all []profilePath
	for _, name := range names {
		for _, raw := range profiles[name].Paths {
			gp, err := path.ParsePath(raw)
			if err != nil {
				return fmt.Errorf("subscription_profiles.%s: path %q: %w", name, raw, err)
			}
			if len(gp.GetElem()) == 0 {
				return fmt.Errorf("subscription_profiles.%s: path %q has no elements", name, raw)
			}
			all = append(all, profilePath{profile: name, raw: raw, elems: gp.GetElem()})
		}
	}

	for i := 0; i < len(all); i++ {
		for j := i + 1; j < len(all); j++ {
			a, b := all[i], all[j]
			if len(a.elems) > len(b.elems) {
				a, b = b, a
			}
			if !prefixIntersects(a.elems, b.elems) {
				continue
			}
			if len(a.elems) == len(b.elems) {
				return fmt.Errorf("oversubscription: duplicate path %q (profile %s) and %q (profile %s) subscribe the same leaves",
					a.raw, a.profile, b.raw, b.profile)
			}
			return fmt.Errorf("oversubscription: parent path %q (profile %s) subsumes %q (profile %s)",
				a.raw, a.profile, b.raw, b.profile)
		}
	}
	return nil
}

// prefixIntersects reports whether the subscription sets of short and long
// intersect, with short's elements compared as a prefix of long's. A key
// present on only one side, or "*" on either side, matches any value.
func prefixIntersects(short, long []*gnmipb.PathElem) bool {
	for i, e := range short {
		o := long[i]
		if e.GetName() != o.GetName() {
			return false
		}
		for k, v := range e.GetKey() {
			ov, ok := o.GetKey()[k]
			if !ok || v == "*" || ov == "*" {
				continue
			}
			if v != ov {
				return false
			}
		}
	}
	return true
}
