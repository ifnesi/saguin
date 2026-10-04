//go:build linux

package main

import "syscall"

// withoutHugePages marks b's pages as never to be backed by a transparent
// huge page, then drops any the kernel has already backed. Under THP
// "always" the kernel backs a whole 2 MB region when anything in it is
// touched (the fault itself, or khugepaged), so a heap object next to the
// floor makes the floor's share of that region resident without the floor
// being written.
//
// **Advising alone is too late for the region the floor starts in.** Heap
// allocated before the floor shares it, and touched it, so the kernel had
// backed it with one huge page before the floor existed. Advising splits
// that mapping and leaves every page of it mapped: 384 of the floor's 2048
// pages on GitHub's runner. MADV_NOHUGEPAGE comes first, so that nothing
// backs a region the floor shares as a huge page again; MADV_DONTNEED then
// drops what is already there. Dropping changes nothing the program can
// see: the floor is zeros and never read, and a dropped page reads as zeros.
func withoutHugePages(b []byte) []byte {
	_ = syscall.Madvise(b, syscall.MADV_NOHUGEPAGE)
	_ = syscall.Madvise(b, syscall.MADV_DONTNEED)
	return b
}
