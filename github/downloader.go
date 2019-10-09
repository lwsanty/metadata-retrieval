package github

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"

	"github.com/src-d/metadata-retrieval/github/graphql"
	"github.com/src-d/metadata-retrieval/github/store"

	"github.com/shurcooL/githubv4"
)

const (
	assigneesPage                 = 2
	issueCommentsPage             = 10
	issuesPage                    = 50
	labelsPage                    = 2
	membersWithRolePage           = 100
	pullRequestReviewCommentsPage = 5
	pullRequestReviewsPage        = 5
	pullRequestsPage              = 50
	repositoryTopicsPage          = 50
)

type storer interface {
	SaveOrganization(organization *graphql.Organization) error
	SaveUser(user *graphql.UserExtended) error
	SaveRepository(repository *graphql.RepositoryFields, topics []string) error
	SaveIssue(repositoryOwner, repositoryName string, issue *graphql.Issue, assignees []string, labels []string) error
	SaveIssueComment(repositoryOwner, repositoryName string, issueNumber int, comment *graphql.IssueComment) error
	SavePullRequest(repositoryOwner, repositoryName string, pr *graphql.PullRequest, assignees []string, labels []string) error
	SavePullRequestComment(repositoryOwner, repositoryName string, pullRequestNumber int, comment *graphql.IssueComment) error
	SavePullRequestReview(repositoryOwner, repositoryName string, pullRequestNumber int, review *graphql.PullRequestReview) error
	SavePullRequestReviewComment(repositoryOwner, repositoryName string, pullRequestNumber int, pullRequestReviewId int, comment *graphql.PullRequestReviewComment) error

	Begin() error
	Commit() error
	Rollback() error
	Version(v int)
	SetActiveVersion(v int) error
	Cleanup(currentVersion int) error
}

// Downloader fetches GitHub data using the v4 API
type Downloader struct {
	storer
	client *githubv4.Client
}

// NewDownloader creates a new Downloader that will store the GitHub metadata
// in the given DB. The HTTP client is expected to have the proper
// authentication setup
func NewDownloader(httpClient *http.Client, db *sql.DB) (*Downloader, error) {
	// TODO: is the ghsync rate limited client needed?

	t := &retryTransport{httpClient.Transport}
	httpClient.Transport = t

	return &Downloader{
		storer: &store.DB{DB: db},
		client: githubv4.NewClient(httpClient),
	}, nil
}

// NewStdoutDownloader creates a new Downloader that will print the GitHub
// metadata to stdout. The HTTP client is expected to have the proper
// authentication setup
func NewStdoutDownloader(httpClient *http.Client) (*Downloader, error) {
	// TODO: is the ghsync rate limited client needed?

	t := &retryTransport{httpClient.Transport}
	httpClient.Transport = t

	return &Downloader{
		storer: &store.Stdout{},
		client: githubv4.NewClient(httpClient),
	}, nil
}

func NewMemDownloader(httpClient *http.Client) (*Downloader, error) {
	// TODO: is the ghsync rate limited client needed?

	t := &retryTransport{httpClient.Transport}
	httpClient.Transport = t

	return &Downloader{
		storer: &store.Mem{
			Repos: make(map[string]map[string]store.Repo),
		},
		client: githubv4.NewClient(httpClient),
	}, nil
}

// DownloadRepository downloads the metadata for the given repository and all
// its resources (issues, PRs, comments, reviews)
func (d Downloader) DownloadRepository(ctx context.Context, owner string, name string, version int) error {
	d.storer.Version(version)

	var err error
	err = d.storer.Begin()
	if err != nil {
		return fmt.Errorf("could not call Begin(): %v", err)
	}

	defer func() {
		if err != nil {
			d.storer.Rollback()
			return
		}

		d.storer.Commit()
	}()

	var q struct {
		graphql.Repository `graphql:"repository(owner: $owner, name: $name)"`
	}

	// Some variables are repeated in the query, like assigneesCursor for Issues
	// and PullRequests. It's ok to reuse because in this top level Repository
	// query the cursors are set to nil, and when the pagination occurs, the
	// queries only request either Issues or PullRequests
	variables := map[string]interface{}{
		"owner": githubv4.String(owner),
		"name":  githubv4.String(name),

		"assigneesPage":                 githubv4.Int(assigneesPage),
		"issueCommentsPage":             githubv4.Int(issueCommentsPage),
		"issuesPage":                    githubv4.Int(issuesPage),
		"labelsPage":                    githubv4.Int(labelsPage),
		"pullRequestReviewCommentsPage": githubv4.Int(pullRequestReviewCommentsPage),
		"pullRequestReviewsPage":        githubv4.Int(pullRequestReviewsPage),
		"pullRequestsPage":              githubv4.Int(pullRequestsPage),
		"repositoryTopicsPage":          githubv4.Int(repositoryTopicsPage),

		"assigneesCursor":                 (*githubv4.String)(nil),
		"issueCommentsCursor":             (*githubv4.String)(nil),
		"issuesCursor":                    (*githubv4.String)(nil),
		"labelsCursor":                    (*githubv4.String)(nil),
		"pullRequestReviewCommentsCursor": (*githubv4.String)(nil),
		"pullRequestReviewsCursor":        (*githubv4.String)(nil),
		"pullRequestsCursor":              (*githubv4.String)(nil),
		"repositoryTopicsCursor":          (*githubv4.String)(nil),
	}

	err = d.client.Query(ctx, &q, variables)
	if err != nil {
		return fmt.Errorf("first query failed: %v", err)
	}

	// repository topics
	topics, err := d.downloadTopics(ctx, &q.Repository)
	if err != nil {
		return err
	}

	err = d.storer.SaveRepository(&q.Repository.RepositoryFields, topics)
	if err != nil {
		return fmt.Errorf("failed to save repository %v: %v", q.Repository.NameWithOwner, err)
	}

	// issues and comments
	err = d.downloadIssues(ctx, owner, name, &q.Repository)
	if err != nil {
		return err
	}

	// PRs and comments
	err = d.downloadPullRequests(ctx, owner, name, &q.Repository)
	if err != nil {
		return err
	}

	return nil
}

// RateRemaining returns the remaining rate limit for the v4 GitHub API
func (d Downloader) RateRemaining(ctx context.Context) (int, error) {
	var q struct {
		RateLimit struct {
			Remaining int
		}
	}

	err := d.client.Query(ctx, &q, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to query remaining rate limit: %v", err)
	}

	return q.RateLimit.Remaining, nil
}

func (d Downloader) downloadTopics(ctx context.Context, repository *graphql.Repository) ([]string, error) {
	topics := []string{}

	// Topics included in the first page
	for _, topicNode := range repository.RepositoryTopics.Nodes {
		topics = append(topics, topicNode.Topic.Name)
	}

	variables := map[string]interface{}{
		"id": githubv4.ID(repository.Id),

		"repositoryTopicsPage":   githubv4.Int(repositoryTopicsPage),
		"repositoryTopicsCursor": (*githubv4.String)(nil),
	}

	// if there are more topics, loop over all the pages
	hasNextPage := repository.RepositoryTopics.PageInfo.HasNextPage
	endCursor := repository.RepositoryTopics.PageInfo.EndCursor

	for hasNextPage {
		// get only repository topics
		var q struct {
			Node struct {
				Repository struct {
					RepositoryTopics graphql.RepositoryTopicsConnection `graphql:"repositoryTopics(first: $repositoryTopicsPage, after: $repositoryTopicsCursor)"`
				} `graphql:"... on Repository"`
			} `graphql:"node(id:$id)"`
		}

		variables["repositoryTopicsCursor"] = githubv4.String(endCursor)

		err := d.client.Query(ctx, &q, variables)
		if err != nil {
			return nil, fmt.Errorf("RepositoryTopics query failed: %v", err)
		}

		for _, topicNode := range q.Node.Repository.RepositoryTopics.Nodes {
			topics = append(topics, topicNode.Topic.Name)
		}

		hasNextPage = q.Node.Repository.RepositoryTopics.PageInfo.HasNextPage
		endCursor = q.Node.Repository.RepositoryTopics.PageInfo.EndCursor
	}

	return topics, nil
}

func (d Downloader) downloadIssues(ctx context.Context, owner string, name string, repository *graphql.Repository) error {
	process := func(issue *graphql.Issue) error {
		assignees, err := d.downloadIssueAssignees(ctx, issue)
		if err != nil {
			return err
		}

		labels, err := d.downloadIssueLabels(ctx, issue)
		if err != nil {
			return err
		}

		err = d.storer.SaveIssue(owner, name, issue, assignees, labels)
		if err != nil {
			return err
		}
		return d.downloadIssueComments(ctx, owner, name, issue)
	}

	// Save issues included in the first page
	for _, issue := range repository.Issues.Nodes {
		err := process(&issue)
		if err != nil {
			return fmt.Errorf("failed to process issue %v/%v #%v: %v", owner, name, issue.Number, err)
		}
	}

	variables := map[string]interface{}{
		"id": githubv4.ID(repository.Id),

		"assigneesPage":     githubv4.Int(assigneesPage),
		"issueCommentsPage": githubv4.Int(issueCommentsPage),
		"issuesPage":        githubv4.Int(issuesPage),
		"labelsPage":        githubv4.Int(labelsPage),

		"assigneesCursor":     (*githubv4.String)(nil),
		"issueCommentsCursor": (*githubv4.String)(nil),
		"issuesCursor":        (*githubv4.String)(nil),
		"labelsCursor":        (*githubv4.String)(nil),
	}

	// if there are more issues, loop over all the pages
	hasNextPage := repository.Issues.PageInfo.HasNextPage
	endCursor := repository.Issues.PageInfo.EndCursor

	for hasNextPage {
		// get only issues
		var q struct {
			Node struct {
				Repository struct {
					Issues graphql.IssueConnection `graphql:"issues(first: $issuesPage, after: $issuesCursor)"`
				} `graphql:"... on Repository"`
			} `graphql:"node(id:$id)"`
		}

		variables["issuesCursor"] = githubv4.String(endCursor)

		err := d.client.Query(ctx, &q, variables)
		if err != nil {
			return fmt.Errorf("failed to query issues for repository %v: %v", repository.NameWithOwner, err)
		}

		for _, issue := range q.Node.Repository.Issues.Nodes {
			err := process(&issue)
			if err != nil {
				return fmt.Errorf("failed to process issue %v #%v: %v", repository.NameWithOwner, issue.Number, err)
			}
		}

		hasNextPage = q.Node.Repository.Issues.PageInfo.HasNextPage
		endCursor = q.Node.Repository.Issues.PageInfo.EndCursor
	}

	return nil
}

func (d Downloader) downloadIssueAssignees(ctx context.Context, issue *graphql.Issue) ([]string, error) {
	assignees := []string{}

	// Assignees included in the first page
	for _, node := range issue.Assignees.Nodes {
		assignees = append(assignees, node.Login)
	}

	variables := map[string]interface{}{
		"id": githubv4.ID(issue.Id),

		"assigneesPage":   githubv4.Int(assigneesPage),
		"assigneesCursor": (*githubv4.String)(nil),
	}

	// if there are more assignees, loop over all the pages
	hasNextPage := issue.Assignees.PageInfo.HasNextPage
	endCursor := issue.Assignees.PageInfo.EndCursor

	for hasNextPage {
		// get only issue assignees
		var q struct {
			Node struct {
				Issue struct {
					Assignees graphql.UserConnection `graphql:"assignees(first: $assigneesPage, after: $assigneesCursor)"`
				} `graphql:"... on Issue"`
			} `graphql:"node(id:$id)"`
		}

		variables["assigneesCursor"] = githubv4.String(endCursor)

		err := d.client.Query(ctx, &q, variables)
		if err != nil {
			return nil, fmt.Errorf("failed to query issue assignees for issue #%v: %v", issue.Number, err)
		}

		for _, node := range q.Node.Issue.Assignees.Nodes {
			assignees = append(assignees, node.Login)
		}

		hasNextPage = q.Node.Issue.Assignees.PageInfo.HasNextPage
		endCursor = q.Node.Issue.Assignees.PageInfo.EndCursor
	}

	return assignees, nil
}

func (d Downloader) downloadIssueLabels(ctx context.Context, issue *graphql.Issue) ([]string, error) {
	labels := []string{}

	// Labels included in the first page
	for _, node := range issue.Labels.Nodes {
		labels = append(labels, node.Name)
	}

	variables := map[string]interface{}{
		"id": githubv4.ID(issue.Id),

		"labelsPage":   githubv4.Int(labelsPage),
		"labelsCursor": (*githubv4.String)(nil),
	}

	// if there are more labels, loop over all the pages
	hasNextPage := issue.Labels.PageInfo.HasNextPage
	endCursor := issue.Labels.PageInfo.EndCursor

	for hasNextPage {
		// get only issue labels
		var q struct {
			Node struct {
				Issue struct {
					Labels graphql.LabelConnection `graphql:"labels(first: $labelsPage, after: $labelsCursor)"`
				} `graphql:"... on Issue"`
			} `graphql:"node(id:$id)"`
		}

		variables["labelsCursor"] = githubv4.String(endCursor)

		err := d.client.Query(ctx, &q, variables)
		if err != nil {
			return nil, fmt.Errorf("failed to query issue labels for issue #%v: %v", issue.Number, err)
		}

		for _, node := range q.Node.Issue.Labels.Nodes {
			labels = append(labels, node.Name)
		}

		hasNextPage = q.Node.Issue.Labels.PageInfo.HasNextPage
		endCursor = q.Node.Issue.Labels.PageInfo.EndCursor
	}

	return labels, nil
}

func (d Downloader) downloadIssueComments(ctx context.Context, owner string, name string, issue *graphql.Issue) error {
	// save first page of comments
	for _, comment := range issue.Comments.Nodes {
		err := d.storer.SaveIssueComment(owner, name, issue.Number, &comment)
		if err != nil {
			return err
		}
	}

	variables := map[string]interface{}{
		"id": githubv4.ID(issue.Id),

		"issueCommentsPage":   githubv4.Int(issueCommentsPage),
		"issueCommentsCursor": (*githubv4.String)(nil),
	}

	// if there are more issue comments, loop over all the pages
	hasNextPage := issue.Comments.PageInfo.HasNextPage
	endCursor := issue.Comments.PageInfo.EndCursor

	for hasNextPage {
		// get only issue comments
		var q struct {
			Node struct {
				Issue struct {
					Comments graphql.IssueCommentsConnection `graphql:"comments(first: $issueCommentsPage, after: $issueCommentsCursor)"`
				} `graphql:"... on Issue"`
			} `graphql:"node(id:$id)"`
		}

		variables["issueCommentsCursor"] = githubv4.String(endCursor)

		err := d.client.Query(ctx, &q, variables)
		if err != nil {
			return fmt.Errorf("failed to query issue comments for issue #%v: %v", issue.Number, err)
		}

		for _, comment := range q.Node.Issue.Comments.Nodes {
			err := d.storer.SaveIssueComment(owner, name, issue.Number, &comment)
			if err != nil {
				return fmt.Errorf("failed to save issue comments for issue #%v: %v", issue.Number, err)
			}
		}

		hasNextPage = q.Node.Issue.Comments.PageInfo.HasNextPage
		endCursor = q.Node.Issue.Comments.PageInfo.EndCursor
	}

	return nil
}

func (d Downloader) downloadPullRequests(ctx context.Context, owner string, name string, repository *graphql.Repository) error {
	process := func(pr *graphql.PullRequest) error {
		assignees, err := d.downloadPullRequestAssignees(ctx, pr)
		if err != nil {
			return err
		}

		labels, err := d.downloadPullRequestLabels(ctx, pr)
		if err != nil {
			return err
		}

		err = d.storer.SavePullRequest(owner, name, pr, assignees, labels)
		if err != nil {
			return err
		}
		err = d.downloadPullRequestComments(ctx, owner, name, pr)
		if err != nil {
			return err
		}
		err = d.downloadPullRequestReviews(ctx, owner, name, pr)
		if err != nil {
			return err
		}

		return nil
	}

	// Save PRs included in the first page
	for _, pr := range repository.PullRequests.Nodes {
		err := process(&pr)
		if err != nil {
			return fmt.Errorf("failed to process PR %v/%v #%v: %v", owner, name, pr.Number, err)
		}
	}

	variables := map[string]interface{}{
		"id": githubv4.ID(repository.Id),

		"assigneesPage":                 githubv4.Int(assigneesPage),
		"issueCommentsPage":             githubv4.Int(issueCommentsPage),
		"labelsPage":                    githubv4.Int(labelsPage),
		"pullRequestReviewCommentsPage": githubv4.Int(pullRequestReviewCommentsPage),
		"pullRequestReviewsPage":        githubv4.Int(pullRequestReviewsPage),
		"pullRequestsPage":              githubv4.Int(pullRequestsPage),

		"assigneesCursor":                 (*githubv4.String)(nil),
		"issueCommentsCursor":             (*githubv4.String)(nil),
		"labelsCursor":                    (*githubv4.String)(nil),
		"pullRequestReviewCommentsCursor": (*githubv4.String)(nil),
		"pullRequestReviewsCursor":        (*githubv4.String)(nil),
		"pullRequestsCursor":              (*githubv4.String)(nil),
	}

	// if there are more PRs, loop over all the pages
	hasNextPage := repository.PullRequests.PageInfo.HasNextPage
	endCursor := repository.PullRequests.PageInfo.EndCursor

	for hasNextPage {
		// get only PRs
		var q struct {
			Node struct {
				Repository struct {
					PullRequests graphql.PullRequestConnection `graphql:"pullRequests(first: $pullRequestsPage, after: $pullRequestsCursor)"`
				} `graphql:"... on Repository"`
			} `graphql:"node(id:$id)"`
		}

		variables["pullRequestsCursor"] = githubv4.String(endCursor)

		err := d.client.Query(ctx, &q, variables)
		if err != nil {
			return fmt.Errorf("failed to query PRs for repository %v/%v: %v", owner, name, err)
		}

		for _, pr := range q.Node.Repository.PullRequests.Nodes {
			err := process(&pr)
			if err != nil {
				return fmt.Errorf("failed to process PR %v/%v #%v: %v", owner, name, pr.Number, err)
			}
		}

		hasNextPage = q.Node.Repository.PullRequests.PageInfo.HasNextPage
		endCursor = q.Node.Repository.PullRequests.PageInfo.EndCursor
	}

	return nil
}

func (d Downloader) downloadPullRequestAssignees(ctx context.Context, pr *graphql.PullRequest) ([]string, error) {
	assignees := []string{}

	// Assignees included in the first page
	for _, node := range pr.Assignees.Nodes {
		assignees = append(assignees, node.Login)
	}

	variables := map[string]interface{}{
		"id": githubv4.ID(pr.Id),

		"assigneesPage":   githubv4.Int(assigneesPage),
		"assigneesCursor": (*githubv4.String)(nil),
	}

	// if there are more assigness, loop over all the pages
	hasNextPage := pr.Assignees.PageInfo.HasNextPage
	endCursor := pr.Assignees.PageInfo.EndCursor

	for hasNextPage {
		// get only PR assignees
		var q struct {
			Node struct {
				PullRequest struct {
					Assignees graphql.UserConnection `graphql:"assignees(first: $assigneesPage, after: $assigneesCursor)"`
				} `graphql:"... on PullRequest"`
			} `graphql:"node(id:$id)"`
		}

		variables["assigneesCursor"] = githubv4.String(endCursor)

		err := d.client.Query(ctx, &q, variables)
		if err != nil {
			return nil, fmt.Errorf("failed to query PR assignees for PR #%v: %v", pr.Number, err)
		}

		for _, node := range q.Node.PullRequest.Assignees.Nodes {
			assignees = append(assignees, node.Login)
		}

		hasNextPage = q.Node.PullRequest.Assignees.PageInfo.HasNextPage
		endCursor = q.Node.PullRequest.Assignees.PageInfo.EndCursor
	}

	return assignees, nil
}

func (d Downloader) downloadPullRequestLabels(ctx context.Context, pr *graphql.PullRequest) ([]string, error) {
	labels := []string{}

	// Labels included in the first page
	for _, node := range pr.Labels.Nodes {
		labels = append(labels, node.Name)
	}

	variables := map[string]interface{}{
		"id": githubv4.ID(pr.Id),

		"labelsPage":   githubv4.Int(assigneesPage),
		"labelsCursor": (*githubv4.String)(nil),
	}

	// if there are more labels, loop over all the pages
	hasNextPage := pr.Labels.PageInfo.HasNextPage
	endCursor := pr.Labels.PageInfo.EndCursor

	for hasNextPage {
		// get only PR labels
		var q struct {
			Node struct {
				PullRequest struct {
					Labels graphql.LabelConnection `graphql:"labels(first: $labelsPage, after: $labelsCursor)"`
				} `graphql:"... on PullRequest"`
			} `graphql:"node(id:$id)"`
		}

		variables["labelsCursor"] = githubv4.String(endCursor)

		err := d.client.Query(ctx, &q, variables)
		if err != nil {
			return nil, fmt.Errorf("failed to query PR labels for PR #%v: %v", pr.Number, err)
		}

		for _, node := range q.Node.PullRequest.Labels.Nodes {
			labels = append(labels, node.Name)
		}

		hasNextPage = q.Node.PullRequest.Labels.PageInfo.HasNextPage
		endCursor = q.Node.PullRequest.Labels.PageInfo.EndCursor
	}

	return labels, nil
}

func (d Downloader) downloadPullRequestComments(ctx context.Context, owner string, name string, pr *graphql.PullRequest) error {
	// save first page of comments
	for _, comment := range pr.Comments.Nodes {
		err := d.storer.SavePullRequestComment(owner, name, pr.Number, &comment)
		if err != nil {
			return fmt.Errorf("failed to save PR comments for PR #%v: %v", pr.Number, err)
		}
	}

	variables := map[string]interface{}{
		"id": githubv4.ID(pr.Id),

		"issueCommentsPage":   githubv4.Int(issueCommentsPage),
		"issueCommentsCursor": (*githubv4.String)(nil),
	}

	// if there are more issue comments, loop over all the pages
	hasNextPage := pr.Comments.PageInfo.HasNextPage
	endCursor := pr.Comments.PageInfo.EndCursor

	for hasNextPage {
		// get only PR comments
		var q struct {
			Node struct {
				PullRequest struct {
					Comments graphql.IssueCommentsConnection `graphql:"comments(first: $issueCommentsPage, after: $issueCommentsCursor)"`
				} `graphql:"... on PullRequest"`
			} `graphql:"node(id:$id)"`
		}

		variables["issueCommentsCursor"] = githubv4.String(endCursor)

		err := d.client.Query(ctx, &q, variables)
		if err != nil {
			return fmt.Errorf("failed to query PR comments for PR #%v: %v", pr.Number, err)
		}

		for _, comment := range q.Node.PullRequest.Comments.Nodes {
			err := d.storer.SavePullRequestComment(owner, name, pr.Number, &comment)
			if err != nil {
				return fmt.Errorf("failed to save PR comments for PR #%v: %v", pr.Number, err)
			}
		}

		hasNextPage = q.Node.PullRequest.Comments.PageInfo.HasNextPage
		endCursor = q.Node.PullRequest.Comments.PageInfo.EndCursor
	}

	return nil
}

func (d Downloader) downloadPullRequestReviews(ctx context.Context, owner string, name string, pr *graphql.PullRequest) error {
	process := func(review *graphql.PullRequestReview) error {
		err := d.storer.SavePullRequestReview(owner, name, pr.Number, review)
		if err != nil {
			return fmt.Errorf("failed to save PR review for PR #%v: %v", pr.Number, err)
		}
		return d.downloadReviewComments(ctx, owner, name, pr.Number, review)
	}

	// save first page of reviews
	for _, review := range pr.Reviews.Nodes {
		err := process(&review)
		if err != nil {
			return err
		}
	}

	variables := map[string]interface{}{
		"id": githubv4.ID(pr.Id),

		"pullRequestReviewCommentsPage": githubv4.Int(pullRequestReviewCommentsPage),
		"pullRequestReviewsPage":        githubv4.Int(pullRequestReviewsPage),

		"pullRequestReviewCommentsCursor": (*githubv4.String)(nil),
		"pullRequestReviewsCursor":        (*githubv4.String)(nil),
	}

	// if there are more reviews, loop over all the pages
	hasNextPage := pr.Reviews.PageInfo.HasNextPage
	endCursor := pr.Reviews.PageInfo.EndCursor

	for hasNextPage {
		// get only PR reviews
		var q struct {
			Node struct {
				PullRequest struct {
					Reviews graphql.PullRequestReviewConnection `graphql:"reviews(first: $pullRequestReviewsPage, after: $pullRequestReviewsCursor)"`
				} `graphql:"... on PullRequest"`
			} `graphql:"node(id:$id)"`
		}

		variables["pullRequestReviewsCursor"] = githubv4.String(endCursor)

		err := d.client.Query(ctx, &q, variables)
		if err != nil {
			return fmt.Errorf("failed to query PR reviews for PR #%v: %v", pr.Number, err)
		}

		for _, review := range q.Node.PullRequest.Reviews.Nodes {
			err := process(&review)
			if err != nil {
				return err
			}
		}

		hasNextPage = q.Node.PullRequest.Reviews.PageInfo.HasNextPage
		endCursor = q.Node.PullRequest.Reviews.PageInfo.EndCursor
	}

	return nil
}

func (d Downloader) downloadReviewComments(ctx context.Context, repositoryOwner, repositoryName string, pullRequestNumber int, review *graphql.PullRequestReview) error {
	process := func(comment *graphql.PullRequestReviewComment) error {
		err := d.storer.SavePullRequestReviewComment(repositoryOwner, repositoryName, pullRequestNumber, review.DatabaseId, comment)
		if err != nil {
			return fmt.Errorf(
				"failed to save PullRequestReviewComment for PR #%v, review ID %v: %v",
				pullRequestNumber, review.Id, err)
		}

		return nil
	}

	// save first page of comments
	for _, comment := range review.Comments.Nodes {
		err := process(&comment)
		if err != nil {
			return err
		}
	}

	variables := map[string]interface{}{
		"id": githubv4.ID(review.Id),

		"pullRequestReviewCommentsPage":   githubv4.Int(pullRequestReviewCommentsPage),
		"pullRequestReviewCommentsCursor": (*githubv4.String)(nil),
	}

	// if there are more review comments, loop over all the pages
	hasNextPage := review.Comments.PageInfo.HasNextPage
	endCursor := review.Comments.PageInfo.EndCursor

	for hasNextPage {
		var q struct {
			Node struct {
				PullRequestReview struct {
					Comments graphql.PullRequestReviewCommentConnection `graphql:"comments(first: $pullRequestReviewCommentsPage, after: $pullRequestReviewCommentsCursor)"`
				} `graphql:"... on PullRequestReview"`
			} `graphql:"node(id:$id)"`
		}

		variables["pullRequestReviewCommentsCursor"] = githubv4.String(endCursor)

		err := d.client.Query(ctx, &q, variables)
		if err != nil {
			return fmt.Errorf(
				"failed to query PR review comments for PR #%v, review ID %v: %v",
				pullRequestNumber, review.Id, err)
		}

		for _, comment := range q.Node.PullRequestReview.Comments.Nodes {
			err := process(&comment)
			if err != nil {
				return err
			}
		}

		hasNextPage = q.Node.PullRequestReview.Comments.PageInfo.HasNextPage
		endCursor = q.Node.PullRequestReview.Comments.PageInfo.EndCursor
	}

	return nil
}

// DownloadOrganization downloads the metadata for the given organization and
// its member users
func (d Downloader) DownloadOrganization(ctx context.Context, name string, version int) error {
	d.storer.Version(version)

	var err error
	err = d.storer.Begin()
	if err != nil {
		return fmt.Errorf("could not call Begin(): %v", err)
	}

	defer func() {
		if err != nil {
			d.storer.Rollback()
			return
		}

		d.storer.Commit()
	}()

	var q struct {
		graphql.Organization `graphql:"organization(login: $organizationLogin)"`
	}

	// Some variables are repeated in the query, like assigneesCursor for Issues
	// and PullRequests. It's ok to reuse because in this top level Repository
	// query the cursors are set to nil, and when the pagination occurs, the
	// queries only request either Issues or PullRequests
	variables := map[string]interface{}{
		"organizationLogin": githubv4.String(name),

		"membersWithRolePage":   githubv4.Int(membersWithRolePage),
		"membersWithRoleCursor": (*githubv4.String)(nil),
	}

	err = d.client.Query(ctx, &q, variables)
	if err != nil {
		return fmt.Errorf("organization query failed: %v", err)
	}

	err = d.storer.SaveOrganization(&q.Organization)
	if err != nil {
		return fmt.Errorf("failed to save organization %v: %v", name, err)
	}

	// issues and comments
	err = d.downloadUsers(ctx, name, &q.Organization)
	if err != nil {
		return err
	}

	return nil
}

func (d Downloader) downloadUsers(ctx context.Context, name string, organization *graphql.Organization) error {
	process := func(user *graphql.UserExtended) error {
		err := d.storer.SaveUser(user)
		if err != nil {
			return fmt.Errorf("failed to save UserExtended: %v", err)
		}

		return nil
	}

	// Save users included in the first page
	for _, user := range organization.MembersWithRole.Nodes {
		err := process(&user)
		if err != nil {
			return fmt.Errorf("failed to process user %v: %v", user.Login, err)
		}
	}

	variables := map[string]interface{}{
		"organizationLogin": githubv4.String(name),

		"membersWithRolePage":   githubv4.Int(membersWithRolePage),
		"membersWithRoleCursor": (*githubv4.String)(nil),
	}

	// if there are more users, loop over all the pages
	hasNextPage := organization.MembersWithRole.PageInfo.HasNextPage
	endCursor := organization.MembersWithRole.PageInfo.EndCursor

	for hasNextPage {
		// get only users
		var q struct {
			Organization struct {
				MembersWithRole graphql.OrganizationMemberConnection `graphql:"membersWithRole(first: $membersWithRolePage, after: $membersWithRoleCursor)"`
			} `graphql:"organization(login: $organizationLogin)"`
		}

		variables["membersWithRoleCursor"] = githubv4.String(endCursor)

		err := d.client.Query(ctx, &q, variables)
		if err != nil {
			return fmt.Errorf("failed to organization members for organization %v: %v", name, err)
		}

		for _, user := range q.Organization.MembersWithRole.Nodes {
			err := process(&user)
			if err != nil {
				return fmt.Errorf("failed to process user %v: %v", user.Login, err)
			}
		}

		hasNextPage = q.Organization.MembersWithRole.PageInfo.HasNextPage
		endCursor = q.Organization.MembersWithRole.PageInfo.EndCursor
	}

	return nil
}

// SetCurrent enables the given version as the current one accessible in the DB
func (d Downloader) SetCurrent(version int) error {
	err := d.storer.SetActiveVersion(version)
	if err != nil {
		return fmt.Errorf("failed to set current DB version to %v: %v", version, err)
	}
	return nil
}

// Cleanup deletes from the DB all records that do not belong to the currentVersion
func (d Downloader) Cleanup(currentVersion int) error {
	err := d.storer.Cleanup(currentVersion)
	if err != nil {
		return fmt.Errorf("failed to do cleanup for DB version %v: %v", currentVersion, err)
	}
	return nil
}
