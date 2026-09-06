package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var ErrExportDestinationConflict = errors.New("export output conflicts with database-owned path")

func ValidateExportDestination(files DatabaseFiles, includeDirectory bool, outputPath string) error {
	outputPath, err := canonicalExportPath(outputPath)
	if err != nil {
		return err
	}
	owned := []string{files.State, files.WAL, files.WALBase, files.IDs, files.State + ".lock", files.State + ".layout"}
	if includeDirectory {
		owned = append(owned, files.Directory)
	}
	prefixes := append(DatabaseTempPrefixes(files, includeDirectory), filepath.Join(filepath.Dir(files.State), ".latticedb-layout-"))
	candidates := []string{outputPath, outputPath + ".lock", outputPath + "_generations"}
	for index, candidate := range candidates {
		candidate, err = canonicalExportPath(candidate)
		if err != nil {
			return err
		}
		candidates[index] = candidate
		for _, path := range owned {
			path, err = canonicalExportPath(path)
			if err != nil {
				return err
			}
			same, err := sameExistingFile(candidate, path)
			if err != nil {
				return err
			}
			if candidate == path || same {
				return ErrExportDestinationConflict
			}
		}
		for _, prefix := range prefixes {
			prefix, err = canonicalExportPath(prefix)
			if err != nil {
				return err
			}
			if strings.HasPrefix(candidate, prefix) {
				return ErrExportDestinationConflict
			}
		}
	}
	for _, path := range owned {
		path, err = canonicalExportPath(path)
		if err != nil {
			return err
		}
		if isWithinExportPath(path, candidates[2]) {
			return ErrExportDestinationConflict
		}
	}
	return nil
}

func sameExistingFile(left, right string) (bool, error) {
	leftInfo, leftErr := os.Stat(left)
	rightInfo, rightErr := os.Stat(right)
	if leftErr != nil && !errors.Is(leftErr, os.ErrNotExist) {
		return false, leftErr
	}
	if rightErr != nil && !errors.Is(rightErr, os.ErrNotExist) {
		return false, rightErr
	}
	return leftErr == nil && rightErr == nil && os.SameFile(leftInfo, rightInfo), nil
}

func isWithinExportPath(path, root string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != "." && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func canonicalExportPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve export output path: %w", err)
	}
	current := abs
	var missing []string
	for {
		canonical, err := filepath.EvalSymlinks(current)
		if err == nil {
			for index := len(missing) - 1; index >= 0; index-- {
				canonical = filepath.Join(canonical, missing[index])
			}
			return canonical, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("resolve export output path: %w", err)
		}
		info, lstatErr := os.Lstat(current)
		if lstatErr == nil && info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(current)
			if err != nil {
				return "", fmt.Errorf("resolve export output path: %w", err)
			}
			if !filepath.IsAbs(target) {
				target = filepath.Join(filepath.Dir(current), target)
			}
			current = filepath.Clean(target)
			continue
		}
		if lstatErr != nil && !errors.Is(lstatErr, os.ErrNotExist) {
			return "", fmt.Errorf("resolve export output path: %w", lstatErr)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", fmt.Errorf("resolve export output path: %w", err)
		}
		missing = append(missing, filepath.Base(current))
		current = parent
	}
}
