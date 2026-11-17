package internal

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

// expandEnv replaces environment variable references in the values of a
// parsed config document:
//
//	$VAR, ${VAR}         the variable; an error if it is not set
//	${VAR:-default}      the variable, or default if it is unset or empty
//	$$                   a literal $
//
// Only values are expanded, never keys or comments. A plain (unquoted) value
// is typed after expansion, so `min_members: ${MONGO_MIN}` gives a number.
func expandEnv(root *yaml.Node, lookup func(string) (string, bool)) error {
	var missing []string
	var walk func(n *yaml.Node)
	walk = func(n *yaml.Node) {
		switch n.Kind {
		case yaml.DocumentNode, yaml.SequenceNode:
			for _, c := range n.Content {
				walk(c)
			}
		case yaml.MappingNode:
			for i := 1; i < len(n.Content); i += 2 {
				walk(n.Content[i])
			}
		case yaml.ScalarNode:
			if !strings.Contains(n.Value, "$") {
				return
			}
			n.Value = os.Expand(n.Value, func(ref string) string {
				if ref == "$" {
					return "$"
				}
				name, def, hasDef := strings.Cut(ref, ":-")
				if v, ok := lookup(name); ok && (v != "" || !hasDef) {
					return v
				}
				if hasDef {
					return def
				}
				if !slices.Contains(missing, name) {
					missing = append(missing, name)
				}
				return ""
			})
			if n.Style == 0 {
				// Let the decoder type the expanded value (int, bool, ...).
				n.Tag = ""
			}
		}
	}
	walk(root)
	if len(missing) > 0 {
		return fmt.Errorf("environment variables not set: %s (use ${NAME:-default} for a default, $$ for a literal $)", strings.Join(missing, ", "))
	}
	return nil
}
