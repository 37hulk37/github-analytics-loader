package com.hulk.loader;

import lombok.Data;
import org.kohsuke.github.GHRepository;

import java.io.IOException;
import java.time.Instant;

@Data
public class RepositoryBasicDto {
    private long id;
    private String name;
    private String fullName;
    private String description;
    private String htmlUrl;
    private String language;
    private int stargazersCount;
    private int forksCount;
    private Instant createdAt;
    private Instant updatedAt;

    private boolean allowForking;
    private boolean allowMergeCommit;
    private boolean allowRebaseMerge;
    private boolean allowSquashMerge;
    private boolean deleteBranchOnMerge;
    private int watchersCount;
    private int size;
    private int openIssuesCount;
    private int subscribersCount;
    private boolean hasIssues;
    private boolean hasWiki;
    private boolean fork;
    private boolean hasDownloads;
    private boolean hasPages;
    private boolean archived;
    private boolean disabled;
    private boolean hasProjects;
    private boolean isPrivate;
    private Boolean isTemplate;
    private String nodeId;
    private String homepage;
    private String defaultBranch;
    private String pushedAt;
    private String visibility;
    private boolean isTemplateFlag;

    private Integer commitsCount;

    public static RepositoryBasicDto fromGHRepository(GHRepository repo, Integer commitsCount) throws IOException {
        RepositoryBasicDto dto = new RepositoryBasicDto();
        dto.setId(repo.getId());
        dto.setName(repo.getName());
        dto.setFullName(repo.getFullName());
        dto.setDescription(repo.getDescription());
        dto.setHtmlUrl(repo.getHtmlUrl().toString());
        dto.setLanguage(repo.getLanguage());
        dto.setStargazersCount(repo.getStargazersCount());
        dto.setForksCount(repo.getForksCount());
        dto.setCreatedAt(repo.getCreatedAt());
        dto.setUpdatedAt(repo.getUpdatedAt());

        // Новые поля
        dto.setAllowForking(repo.isAllowForking());
        dto.setAllowMergeCommit(repo.isAllowMergeCommit());
        dto.setAllowRebaseMerge(repo.isAllowRebaseMerge());
        dto.setAllowSquashMerge(repo.isAllowSquashMerge());
        dto.setDeleteBranchOnMerge(repo.isDeleteBranchOnMerge());
        dto.setWatchersCount(repo.getWatchersCount());
        dto.setSize(repo.getSize());
        dto.setOpenIssuesCount(repo.getOpenIssueCount());
        dto.setSubscribersCount(repo.getSubscribersCount());
        dto.setHasIssues(repo.hasIssues());
        dto.setHasWiki(repo.hasWiki());
        dto.setFork(repo.isFork());
        dto.setHasDownloads(repo.hasDownloads());
        dto.setHasPages(repo.hasPages());
        dto.setArchived(repo.isArchived());
        dto.setDisabled(repo.isDisabled());
        dto.setHasProjects(repo.hasProjects());
        dto.setPrivate(repo.isPrivate());
        dto.setIsTemplate(repo.isTemplate());
        dto.setNodeId(repo.getNodeId());
        dto.setHomepage(repo.getHomepage());
        dto.setDefaultBranch(repo.getDefaultBranch());
        dto.setPushedAt(repo.getPushedAt() != null ? repo.getPushedAt().toString() : null);
        dto.setVisibility(repo.getVisibility().toString());
        dto.setTemplateFlag(repo.isTemplate());

        dto.setCommitsCount(commitsCount);

        return dto;
    }
}