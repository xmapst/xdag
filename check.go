package xdag

import "slices"

func HasCycle(tasks map[string]Task) bool {
    visited := make(map[string]bool)
    recStack := make(map[string]bool)
    
    var dfs func(taskName string) bool
    dfs = func(taskName string) bool {
        if recStack[taskName] {
            // Cycle detected
            return true
        }
        if visited[taskName] {
            // Already processed
            return false
        }
        
        visited[taskName] = true
        recStack[taskName] = true
        
        if slices.ContainsFunc(tasks[taskName].Dependencies(), dfs) {
            return true
        }
        
        recStack[taskName] = false
        return false
    }
    
    for taskName := range tasks {
        if !visited[taskName] {
            if dfs(taskName) {
                return true
            }
        }
    }
    
    return false
}
