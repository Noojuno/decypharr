//go:build windows

package manager

import "golang.org/x/sys/windows"

func availableBytes(path string) (uint64, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var freeAvailable, totalBytes, totalFree uint64
	if err := windows.GetDiskFreeSpaceEx(p, &freeAvailable, &totalBytes, &totalFree); err != nil {
		return 0, err
	}
	return freeAvailable, nil
}
