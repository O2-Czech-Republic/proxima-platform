    
    DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);

    @Override
    public DirectDataOperator getOperator() {
      return direct;
    }

    public Optional<CommitLogReader> getCommitLog(AttributeDescriptor<?>... attributes) {
      Set<DirectAttributeFamilyDescriptor> descriptors = new HashSet<>();
      for (AttributeDescriptor<?> attr : attributes) {
        Set<DirectAttributeFamilyDescriptor> commitLogs = direct.getFamiliesForAttribute(attr)
            .stream()
            .filter(af -> af.getDesc().getAccess().canReadCommitLog())
            .filter(af -> af.getCommitLogReader().isPresent())
            .collect(Collectors.toSet());
        if (commitLogs.isEmpty()) {
          return Optional.empty();
        }
        descriptors.addAll(commitLogs);
      }
      // FIXME: we cannot join multiple commit logs together for now
      // so we have to throw exception here
      if (descriptors.size() > 1) {
        throw new UnsupportedOperationException("Unsupported: multiple commit logs for multiple attributes");
      }
      return descriptors.stream().findAny().get().getCommitLogReader();
    }
