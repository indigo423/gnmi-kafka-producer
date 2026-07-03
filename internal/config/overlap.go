// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"maps"
	"slices"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/openconfig/gnmic/pkg/api/path"
)

// Oversubscription validation: reject a target whose bound profiles' combined
// paths make the device stream the same leaf more than once — either the same
// path twice, or a parent container plus one of its own leaves. Profiles that
// overlap but are never bound to the same target are legal. Paths are parsed
// with the same gnmic parser the subscribe builder uses (api.Path), so a path
// that fails here would also fail at subscribe time and vice versa. Keys are
// matched with wildcard semantics: an absent key or "*" on either side
// intersects any value.

type profilePath struct {
	profile string
	raw     string
	elems   []*gnmipb.PathElem
}

// parseProfilePaths parses every profile's paths once, so parse errors surface
// even for profiles no target binds.
func parseProfilePaths(profiles map[string]SubscriptionProfile) (map[string][]profilePath, error) {
	parsed := make(map[string][]profilePath, len(profiles))
	for _, name := range slices.Sorted(maps.Keys(profiles)) {
		for _, raw := range profiles[name].Paths {
			gp, err := path.ParsePath(raw)
			if err != nil {
				return nil, fmt.Errorf("subscription_profiles.%s: path %q: %w", name, raw, err)
			}
			if len(gp.GetElem()) == 0 {
				return nil, fmt.Errorf("subscription_profiles.%s: path %q has no elements", name, raw)
			}
			parsed[name] = append(parsed[name], profilePath{profile: name, raw: raw, elems: gp.GetElem()})
		}
	}
	return parsed, nil
}

// validateNoOverlap checks the union of one target's bound profiles pairwise.
func validateNoOverlap(target string, bound []string, parsed map[string][]profilePath) error {
	var all []profilePath
	for _, name := range bound {
		all = append(all, parsed[name]...)
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
				return fmt.Errorf("targets.%s: oversubscription: duplicate path %q (profile %s) and %q (profile %s) subscribe the same leaves",
					target, a.raw, a.profile, b.raw, b.profile)
			}
			return fmt.Errorf("targets.%s: oversubscription: parent path %q (profile %s) subsumes %q (profile %s)",
				target, a.raw, a.profile, b.raw, b.profile)
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
