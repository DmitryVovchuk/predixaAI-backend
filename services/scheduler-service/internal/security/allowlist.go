package security

type Allowlist struct {
	Schemas []string
	Tables  []string
}

func (a Allowlist) AllowsTable(table string) bool {
	if len(a.Tables) == 0 {
		return true
	}
	for _, t := range a.Tables {
		if t == table {
			return true
		}
	}
	return false
}
