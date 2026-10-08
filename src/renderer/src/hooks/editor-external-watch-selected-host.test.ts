import { expect, it } from 'vitest'
import type { AppState } from '@/store/types'
import { makeFolderWorkspace, makeWorktree } from '@/store/slices/worktrees-slice-test-fixtures'
import { getDefaultSettings } from '../../../shared/constants'
import {
  selectEditorExternalWatchTargets,
  type EditorExternalWatchTargetState
} from './editor-external-watch-targets'

type SelectedHostState = EditorExternalWatchTargetState &
  Pick<AppState, 'activeWorkspaceExecutionHostId'>

const hosts = ['host-a', 'host-b'] as const
function makeState(workspace: 'worktree' | 'folder'): SelectedHostState {
  const id = workspace === 'folder' ? 'folder:same-folder' : 'same-worktree'
  return {
    settings: getDefaultSettings('/home/me'),
    openFiles: [],
    activeWorktreeId: id,
    activeWorkspaceExecutionHostId: 'runtime:host-a',
    repos: [],
    worktreesByRepo:
      workspace === 'worktree'
        ? {
            repo: hosts.map((host) =>
              makeWorktree({ id, repoId: 'repo', path: '/repo', hostId: `runtime:${host}` })
            )
          }
        : {},
    folderWorkspaces:
      workspace === 'folder'
        ? hosts.map((host) =>
            makeFolderWorkspace({
              id: 'same-folder',
              folderPath: '/folder',
              executionHostId: `runtime:${host}`
            })
          )
        : [],
    projectGroups: [],
    rightSidebarOpen: true,
    rightSidebarTab: 'explorer',
    rightSidebarExplorerView: 'files',
    gitStatusHugeByWorktree: {},
    sshConnectionStates: new Map()
  }
}

it.each(['worktree', 'folder'] as const)(
  'changes the %s watcher when only the selected execution host changes',
  (workspace) => {
    const state = makeState(workspace)
    const first = selectEditorExternalWatchTargets(state)
    expect(first.targets.map((target) => target.runtimeEnvironmentId)).toEqual(['host-a'])
    const switched = selectEditorExternalWatchTargets({
      ...state,
      activeWorkspaceExecutionHostId: 'runtime:host-b'
    })
    expect(switched.targets.map((target) => target.runtimeEnvironmentId)).toEqual(['host-b'])
    expect(switched.targetsKey).not.toBe(first.targetsKey)
    expect(selectEditorExternalWatchTargets(state).targetsKey).toBe(first.targetsKey)
  }
)

it.each(['worktree', 'folder'] as const)(
  'preserves the %s watcher snapshot when selection and watched roots stay unchanged',
  (workspace) => {
    const state = makeState(workspace)
    const first = selectEditorExternalWatchTargets(state)
    expect(selectEditorExternalWatchTargets({ ...state })).toBe(first)
  }
)
