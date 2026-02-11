package security

import "regexp"

var identRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func IsSafeIdentifier(value string) bool {
	return identRegex.MatchString(value)
}
