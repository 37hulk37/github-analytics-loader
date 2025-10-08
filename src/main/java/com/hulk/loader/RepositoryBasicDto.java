package com.hulk.loader;

import lombok.Data;
import org.kohsuke.github.GHRepository;

import java.io.IOException;
import java.time.Instant;

//todo: add fields to DTO (for now using only fields that do not trigger additional API calls)

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

    public static RepositoryBasicDto fromGHRepository(GHRepository repo) throws IOException {
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
        return dto;
    }
}