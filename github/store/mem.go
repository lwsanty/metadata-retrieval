package store

import (
	"fmt"
	"sync"

	"github.com/src-d/metadata-retrieval/github/graphql"
)

var NotFound = fmt.Errorf("not found")

// TODO mutex
// one repo
type Mem struct {
	mu    sync.Mutex
	Repos map[string]map[string]Repo
}

type Repo struct {
	RepositoryFields graphql.RepositoryFields
	Topics           []string
	PRs              map[int]PullRequest
}

type PullRequest struct {
	PullRequest graphql.PullRequest
	Assignees   []string
	Labels      []string
	Comments    []graphql.IssueComment
	Reviews     map[int]PullRequestReview
}

type PullRequestReview struct {
	PullRequestReview graphql.PullRequestReview
	Comments          []graphql.PullRequestReviewComment
}

func (m *Mem) SaveOrganization(organization *graphql.Organization) error {
	fmt.Printf("organization data fetched for %s\n", organization.Login)
	return nil
}

func (m *Mem) SaveUser(user *graphql.UserExtended) error {
	fmt.Printf("user data fetched for %s\n", user.Login)
	return nil
}

func (m *Mem) SaveRepository(repository *graphql.RepositoryFields, topics []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Printf("repository data fetched for %s/%s\n", repository.Owner.Login, repository.Name)

	if _, ok := m.Repos[repository.Owner.Login]; !ok {
		m.Repos[repository.Owner.Login] = make(map[string]Repo)
	}

	m.Repos[repository.Owner.Login][repository.Name] = Repo{
		RepositoryFields: *repository,
		Topics:           topics,
		PRs:              make(map[int]PullRequest),
	}
	return nil
}

func (m *Mem) SaveIssue(repositoryOwner, repositoryName string, issue *graphql.Issue, assignees []string, labels []string) error {
	fmt.Printf("issue data fetched for #%v %s\n", issue.Number, issue.Title)
	return nil
}

func (m *Mem) SaveIssueComment(repositoryOwner, repositoryName string, issueNumber int, comment *graphql.IssueComment) error {
	fmt.Printf("  issue comment data fetched by %s at %v: %q\n", comment.Author.Login, comment.CreatedAt, trim(comment.Body))
	return nil
}

func (m *Mem) SavePullRequest(repositoryOwner, repositoryName string, pr *graphql.PullRequest, assignees []string, labels []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Printf("PR data fetched for #%v %s\n", pr.Number, pr.Title)

	if _, ok := m.Repos[repositoryOwner][repositoryName]; !ok {
		return NotFound
	}

	m.Repos[repositoryOwner][repositoryName].PRs[pr.Number] = PullRequest{
		PullRequest: *pr,
		Assignees:   assignees,
		Labels:      labels,
		Reviews:     make(map[int]PullRequestReview),
	}

	return nil
}

func (m *Mem) SavePullRequestComment(repositoryOwner, repositoryName string, pullRequestNumber int, comment *graphql.IssueComment) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Printf("  pr comment data fetched by %s at %v: %q\n", comment.Author.Login, comment.CreatedAt, trim(comment.Body))

	tmpPR, ok := m.Repos[repositoryOwner][repositoryName].PRs[pullRequestNumber]
	if !ok {
		return NotFound
	}
	tmpPR.Comments = append(tmpPR.Comments, *comment)
	m.Repos[repositoryOwner][repositoryName].PRs[pullRequestNumber] = tmpPR

	return nil
}

func (m *Mem) SavePullRequestReview(repositoryOwner, repositoryName string, pullRequestNumber int, review *graphql.PullRequestReview) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Printf("  PR Review data fetched by %s at %v: %q\n", review.Author.Login, review.SubmittedAt, trim(review.Body))

	tmpPR, ok := m.Repos[repositoryOwner][repositoryName].PRs[pullRequestNumber]
	if !ok {
		return NotFound
	}
	tmpPR.Reviews[review.DatabaseId] = PullRequestReview{
		PullRequestReview: *review,
	}
	m.Repos[repositoryOwner][repositoryName].PRs[pullRequestNumber] = tmpPR
	return nil
}

func (m *Mem) SavePullRequestReviewComment(repositoryOwner, repositoryName string, pullRequestNumber int, pullRequestReviewId int, comment *graphql.PullRequestReviewComment) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Printf("    PR review comment data fetched by %s at %v: %q\n", comment.Author.Login, comment.CreatedAt, trim(comment.Body))

	tmpReview, ok := m.Repos[repositoryOwner][repositoryName].PRs[pullRequestNumber].Reviews[pullRequestReviewId]
	if !ok {
		return NotFound
	}

	tmpReview.Comments = append(tmpReview.Comments, *comment)
	m.Repos[repositoryOwner][repositoryName].PRs[pullRequestNumber].Reviews[pullRequestReviewId] = tmpReview

	return nil
}

func (m *Mem) Begin() error {
	return nil
}

func (m *Mem) Commit() error {
	return nil
}

func (m *Mem) Rollback() error {
	return nil
}

func (m *Mem) Version(v int) {
}

func (m *Mem) SetActiveVersion(v int) error {
	return nil
}

func (m *Mem) Cleanup(currentVersion int) error {
	return nil
}
