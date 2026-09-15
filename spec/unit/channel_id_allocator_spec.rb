require "spec_helper"

RSpec.describe AsyncRabbitMQ::ChannelIdAllocator do
  it "hands out ids from 1 upwards and gives the lowest free one back first" do
    ids = described_class.new(8)
    expect(4.times.map { ids.allocate }).to eq([1, 2, 3, 4])

    ids.release(2)
    expect(ids.allocate).to eq(2)
    expect(ids.allocate).to eq(5)
  end

  it "returns nil when every id up to the limit is taken, then recovers after a release" do
    ids = described_class.new(4)
    expect(4.times.map { ids.allocate }).to eq([1, 2, 3, 4])
    expect(ids.allocate).to be_nil
    expect(ids.allocate).to be_nil

    ids.release(4)
    expect(ids.allocate).to eq(4)
  end

  it "never hands out an id above the limit, whatever the limit's shape" do
    [1, 4, 63, 64, 65, 2047].each do |limit|
      ids = described_class.new(limit)
      handed = []
      while (id = ids.allocate)
        handed << id
      end
      expect(handed).to eq((1..limit).to_a), "limit #{limit}"
    end
  end

  it "reserves a specific id once, and reports what is allocated" do
    ids = described_class.new(8)
    expect(ids.reserve(3)).to be true
    expect(ids.reserve(3)).to be false
    expect(ids.allocated?(3)).to be true
    expect(ids.allocated?(4)).to be false
    expect(ids.allocate).to eq(1)
    expect(ids.reserve(9)).to be false # outside the range
  end

  it "ignores a release outside the range and resets cleanly" do
    ids = described_class.new(4)
    ids.allocate
    ids.release(0)
    ids.release(99)
    ids.reset
    expect(ids.allocate).to eq(1)
  end

  it "refuses a limit of zero" do
    expect { described_class.new(0) }.to raise_error(ArgumentError, /positive/)
  end
end
