package merge

import (
	"testing"

	"github.com/pg-branch/pg-branch/internal/diff"
	"github.com/pg-branch/pg-branch/internal/tracker"
)

func TestSplitObjectName(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"public.users.email", []string{"public.users.email", "email", "users.email"}},
		{"users", []string{"users"}},
		{"public.users", []string{"public.users", "users"}},
		{"", []string{""}},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got := splitObjectName(c.in)
			if len(got) != len(c.want) {
				t.Fatalf("len: got %d (%v), want %d (%v)", len(got), got, len(c.want), c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Errorf("[%d]: got %q, want %q", i, got[i], c.want[i])
				}
			}
		})
	}
}

func TestFindDDLForObjectReturnsMostRecent(t *testing.T) {
	log := []tracker.DDLEntry{
		{ID: 1, CommandTag: "CREATE TABLE", ObjectIdentity: "public.users", Command: "CREATE TABLE public.users (id INT)"},
		{ID: 2, CommandTag: "ALTER TABLE", ObjectIdentity: "public.users", Command: "ALTER TABLE public.users ADD COLUMN bio TEXT"},
		{ID: 3, CommandTag: "CREATE TABLE", ObjectIdentity: "public.posts", Command: "CREATE TABLE public.posts (id INT)"},
	}
	got := findDDLForObject(log, "public.users")
	want := "ALTER TABLE public.users ADD COLUMN bio TEXT"
	if got != want {
		t.Errorf("expected most recent match, got %q", got)
	}
}

func TestFindDDLForObjectFallsBackToCommandSubstring(t *testing.T) {
	log := []tracker.DDLEntry{
		{ID: 1, CommandTag: "CREATE INDEX", ObjectIdentity: "", Command: "CREATE INDEX idx_users_email ON public.users (email)"},
	}
	// object_identity is empty (trigger sometimes doesn't populate it for indexes);
	// the fallback should still match by substring on the command.
	got := findDDLForObject(log, "public.idx_users_email")
	if got == "" {
		t.Error("expected fallback substring match to return the CREATE INDEX command")
	}
}

func TestFindDDLForObjectNoMatch(t *testing.T) {
	log := []tracker.DDLEntry{
		{ID: 1, CommandTag: "CREATE TABLE", ObjectIdentity: "public.users", Command: "CREATE TABLE public.users (id INT)"},
	}
	if got := findDDLForObject(log, "public.missing_table"); got != "" {
		t.Errorf("expected empty string for no match, got %q", got)
	}
}

func TestCoveredByPgDump(t *testing.T) {
	dumps := map[string]string{
		"public.widgets": `CREATE TABLE public.widgets (id int);
ALTER TABLE ONLY public.widgets ADD CONSTRAINT widgets_pkey PRIMARY KEY (id);
CREATE INDEX idx_widgets_name ON public.widgets (name);`,
	}

	cases := []struct {
		name       string
		objectName string
		want       bool
	}{
		{"pk included in dump", "public.widgets_pkey", true},
		{"index included in dump", "public.idx_widgets_name", true},
		{"unrelated object", "public.orders_fk", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := coveredByPgDump(c.objectName, dumps); got != c.want {
				t.Errorf("got %v, want %v", got, c.want)
			}
		})
	}
}

func TestSortChangesPutsTablesFirst(t *testing.T) {
	changes := []diff.SchemaChange{
		{ObjectKind: "index", ObjectName: "public.idx_a"},
		{ObjectKind: "table", ObjectName: "public.widgets"},
		{ObjectKind: "constraint", ObjectName: "public.widgets_pkey"},
		{ObjectKind: "column", ObjectName: "public.widgets.name"},
	}
	sortChanges(changes)

	// tables first, then columns, then indexes/constraints together
	if changes[0].ObjectKind != "table" {
		t.Errorf("expected table first, got %q", changes[0].ObjectKind)
	}
	if changes[1].ObjectKind != "column" {
		t.Errorf("expected column second, got %q", changes[1].ObjectKind)
	}
	// positions 2 and 3 are index/constraint in some order — both have rank 2
	for _, c := range changes[2:] {
		if c.ObjectKind != "index" && c.ObjectKind != "constraint" {
			t.Errorf("expected index or constraint in tail, got %q", c.ObjectKind)
		}
	}
}

func TestContainsObjectRefMatchesDottedSuffix(t *testing.T) {
	// A CREATE INDEX command might reference just the bare index name;
	// containsObjectRef should find it via the suffix split.
	command := "CREATE INDEX idx_widgets_name ON public.widgets (name)"
	if !containsObjectRef(command, "public.idx_widgets_name") {
		t.Error("expected suffix match for bare index name in CREATE INDEX command")
	}
}

func TestContainsObjectRefNoMatch(t *testing.T) {
	command := "CREATE TABLE public.other (id INT)"
	if containsObjectRef(command, "public.widgets") {
		t.Error("should not match unrelated objects")
	}
}
